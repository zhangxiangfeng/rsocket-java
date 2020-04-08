package io.rsocket;

import static io.rsocket.fragmentation.FragmentationUtils.isFragmentable;
import static io.rsocket.fragmentation.FragmentationUtils.isValid;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.fragmentation.ReassemblyUtils;
import io.rsocket.frame.CancelFrameFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class RequestStreamFlux extends Flux<Payload> implements Reassemble<Payload>, Scannable {

  final ByteBufAllocator allocator;
  final Payload payload;
  final int mtu;
  final StateAware parent;
  final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<Reassemble<?>> activeSubscribers;
  final UnboundedProcessor<ByteBuf> sendProcessor;
  final PayloadDecoder payloadDecoder;

  static final long FLAG_REASSEMBLY =
      0b0_1__00000000000000000000000000000000000000000000000000000000000000L;
  static final long FLAG_INVERT_REASSEMBLY = ~FLAG_REASSEMBLY;

  static final long MASK_REQUESTED =
      0b0_0__11111111111111111111111111111111111111111111111111111111111111L;
  static final long MASK_REQUESTED_MAX =
      0b1_0__10000000000000000000000000000000000000000000000000000000000000L;
  static final long MASK_FLAGS =
      0b1_1__00000000000000000000000000000000000000000000000000000000000000L;

  static final long STATE_UNSUBSCRIBED =
      0b1_0000000000000000000000000000000000000000000000000000000000000__10L;
  static final long STATE_SUBSCRIBED =
      0b1_0000000000000000000000000000000000000000000000000000000000000__01L;
  static final long STATE_TERMINATED =
      0b1_0__00000000000000000000000000000000000000000000000000000000000000L;

  volatile long requested;
  static final AtomicLongFieldUpdater<RequestStreamFlux> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(RequestStreamFlux.class, "requested");

  int streamId;
  CoreSubscriber<? super Payload> actual;
  CompositeByteBuf frames;
  boolean done;

  RequestStreamFlux(
      ByteBufAllocator allocator,
      Payload payload,
      int mtu,
      StateAware parent,
      StreamIdSupplier streamIdSupplier,
      IntObjectMap<Reassemble<?>> activeSubscribers,
      UnboundedProcessor<ByteBuf> sendProcessor,
      PayloadDecoder payloadDecoder) {
    this.allocator = allocator;
    this.payload = payload;
    this.mtu = mtu;
    this.parent = parent;
    this.streamIdSupplier = streamIdSupplier;
    this.activeSubscribers = activeSubscribers;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;

    REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
  }

  @Override
  @NonNull
  public Context currentContext() {
    long state = this.requested;

    if (state == STATE_UNSUBSCRIBED) {
      return Context.empty();
    }

    return this.actual.currentContext();
  }

  @Override
  public final void onSubscribe(Subscription subscription) {
    subscription.cancel();
    // TODO: Add logging
  }

  @Override
  public final void onComplete() {
    if (this.requested == STATE_TERMINATED) {
      return;
    }

    this.done = true;

    if (REQUESTED.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      return;
    }

    this.activeSubscribers.remove(this.streamId, this);

    this.actual.onComplete();
  }

  @Override
  public final void onError(Throwable cause) {
    if (this.requested == STATE_TERMINATED) {
      Operators.onErrorDropped(cause, currentContext());
      return;
    }

    final CompositeByteBuf frames = this.frames;
    this.frames = null;
    this.done = true;

    if (REQUESTED.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      Operators.onErrorDropped(cause, currentContext());
      return;
    }

    if (frames != null && frames.refCnt() > 0) {
      frames.release();
    }

    this.activeSubscribers.remove(this.streamId, this);

    this.actual.onError(cause);
  }

  @Override
  public final void onNext(Payload p) {
    if (this.requested == STATE_TERMINATED) {
      p.release();
      return;
    }

    this.actual.onNext(p);
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    final Payload p = this.payload;

    if (p.refCnt() > 0) {
      if (this.requested == STATE_UNSUBSCRIBED
          && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBED)) {
        this.actual = actual;

        final int mtu = this.mtu;
        final boolean hasMetadata = p.hasMetadata();
        final ByteBuf metadata = p.metadata();
        final ByteBuf data = p.data();

        if (hasMetadata ? !isValid(mtu, data, metadata) : !isValid(mtu, data)) {
          Operators.error(actual, new IllegalArgumentException("Too Big Payload size"));
          p.release();
          return;
        }

        // call onSubscribe if has value in the result or no result delivered so far
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual, new IllegalStateException("RequestStreamFlux allows only a single Subscriber"));
      }
    } else {
      Operators.error(actual, new IllegalReferenceCountException(0));
    }
  }

  @Override
  public final void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    long state;
    long currentRequested;
    long nextRequested;
    for (; ; ) {
      state = this.requested;

      if (state == STATE_TERMINATED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        currentRequested = 0;
        nextRequested = Math.min(n, MASK_REQUESTED);

        if (REQUESTED.compareAndSet(this, state, nextRequested)) {
          break;
        }
      } else {
        // first request has happened with Long.MAX_VALUE
        if ((state & MASK_REQUESTED_MAX) == MASK_REQUESTED_MAX) {
          return;
        }

        // racing on request(Long.MAX) and request(Long.MAX)
        if ((currentRequested = (state & MASK_REQUESTED)) == MASK_REQUESTED) {
          return;
        }

        nextRequested = Math.min(Operators.addCap(currentRequested, n), MASK_REQUESTED);

        if (REQUESTED.compareAndSet(this, state, nextRequested | (state & MASK_FLAGS))) {
          break;
        }
      }
    }

    if (currentRequested > 0) {
      return;
    }

    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;

    int streamId = this.streamId;

    for (; ; ) {
      if (state == STATE_SUBSCRIBED) {
        final Throwable throwable = this.parent.checkAvailable();
        if (throwable != null) {
          this.payload.release();
          this.requested = STATE_TERMINATED;
          this.actual.onError(throwable);
          return;
        }

        final IntObjectMap<Reassemble<?>> as = this.activeSubscribers;
        final Payload p = this.payload;
        final int mtu = this.mtu;

        try {
          final boolean hasMetadata = p.hasMetadata();
          final ByteBuf metadata = p.metadata();
          final ByteBuf data = p.data();

          streamId = this.streamIdSupplier.nextStreamId(as);
          this.streamId = streamId;

          if (hasMetadata ? isFragmentable(mtu, data, metadata) : isFragmentable(mtu, data)) {
            final ByteBuf slicedData = data.slice();
            final ByteBuf slicedMetadata = hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;

            final ByteBuf first =
                FragmentationUtils.encodeFirstFragment(
                    allocator,
                    mtu,
                    (int) Math.min(nextRequested, Integer.MAX_VALUE),
                    FrameType.REQUEST_STREAM,
                    streamId,
                    slicedMetadata,
                    slicedData);

            as.put(streamId, this);
            sender.onNext(first);

            while (slicedData.isReadable() || slicedMetadata.isReadable()) {
              final ByteBuf following =
                  FragmentationUtils.encodeFollowsFragment(
                      allocator, mtu, streamId, false, slicedMetadata, slicedData);
              sender.onNext(following);
            }
          } else {
            final ByteBuf retainedSlicedData = data.retainedSlice();
            final ByteBuf retainedSlicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

            final ByteBuf requestFrame =
                RequestStreamFrameFlyweight.encode(
                    allocator,
                    streamId,
                    false,
                    (int) Math.min(nextRequested, Integer.MAX_VALUE),
                    retainedSlicedMetadata,
                    retainedSlicedData);

            as.put(streamId, this);
            sender.onNext(requestFrame);
          }

          p.release();
        } catch (Throwable e) {
          this.done = true;
          this.requested = STATE_TERMINATED;

          ReferenceCountUtil.safeRelease(p);
          Exceptions.throwIfFatal(e);

          if (as.remove(streamId, this)) {
            final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
            sender.onNext(cancelFrame);
          }

          this.actual.onError(e);

          return;
        }
      } else {
        final ByteBuf requestNFrame =
            RequestNFrameFlyweight.encode(allocator, streamId, nextRequested);
        sender.onNext(requestNFrame);
      }

      for (; ; ) {
        long toUpdate;
        // now currentRequested is newer than nextRequested
        state = this.requested;

        if (state == STATE_TERMINATED) {
          // we should not override terminal state
          nextRequested = STATE_TERMINATED;
          break;
        }

        if (nextRequested == MASK_REQUESTED) {
          // we should state that it max value has already been requested, so no need to loop
          // anymore
          toUpdate = MASK_REQUESTED_MAX;
        } else {
          // subtract the requestN from the latest requested state
          currentRequested = state & MASK_REQUESTED;
          toUpdate = currentRequested - nextRequested;
        }

        if (REQUESTED.compareAndSet(this, state, toUpdate | (state & MASK_FLAGS))) {
          nextRequested = toUpdate;
          break;
        }
      }

      // was terminated while looping. Need to check is was cancelled
      if (nextRequested == STATE_TERMINATED) {
        // done is false if was terminated because of cancellation
        if (!this.done) {
          this.activeSubscribers.remove(streamId, this);

          final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
          sender.onNext(cancelFrame);
        }

        return;
      }

      // already requested max. No need to do anything else
      if (nextRequested == MASK_REQUESTED_MAX) {
        return;
      }

      // all good, just exit
      if (nextRequested == 0) {
        return;
      }

      // repeat, because was requested more while we were sending frame
    }
  }

  @Override
  public final void cancel() {
    if (this.requested == STATE_TERMINATED) {
      return;
    }

    final long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state != STATE_TERMINATED) {

      if (state == STATE_SUBSCRIBED) {
        // no need to send anything, since the first request has not happened
        this.payload.release();
        return;
      }

      if ((state & MASK_REQUESTED) == 0 || (state & MASK_REQUESTED_MAX) == MASK_REQUESTED_MAX) {
        final int streamId = this.streamId;
        this.activeSubscribers.remove(streamId, this);

        final CompositeByteBuf frames = this.frames;
        this.frames = null;
        if (frames != null && frames.refCnt() > 0) {
          ReferenceCountUtil.safeRelease(frames);
        }

        final ByteBuf cancelFrame = CancelFrameFlyweight.encode(this.allocator, streamId);
        this.sendProcessor.onNext(cancelFrame);
      }
    }
  }

  @Override
  public boolean isReassemblingNow() {
    return this.frames != null;
  }

  @Override
  public void reassemble(ByteBuf followingFrame, boolean hasFollows, boolean terminal) {
    if (this.requested == STATE_TERMINATED) {
      return;
    }

    CompositeByteBuf frames = this.frames;

    if (frames == null) {
      frames = ReassemblyUtils.addFollowingFrame(this.allocator.compositeBuffer(), followingFrame);
      this.frames = frames;

      for (; ; ) {
        long state = this.requested;

        if (state == STATE_TERMINATED) {
          this.frames = null;
          ReferenceCountUtil.safeRelease(frames);
        }

        if (REQUESTED.compareAndSet(this, state, state | FLAG_REASSEMBLY)) {
          return;
        }
      }
    } else {
      frames = ReassemblyUtils.addFollowingFrame(frames, followingFrame);
    }

    if (!hasFollows) {
      this.frames = null;
      for (; ; ) {
        long state = this.requested;

        if (state == STATE_TERMINATED) {
          ReferenceCountUtil.safeRelease(frames);
          return;
        }

        if (REQUESTED.compareAndSet(this, state, state & FLAG_INVERT_REASSEMBLY)) {
          break;
        }
      }
      try {
        this.onNext(this.payloadDecoder.apply(frames));
        frames.release();

        if (terminal) {
          this.onComplete();
        }
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);
        Exceptions.throwIfFatal(t);

        if (this.requested != STATE_TERMINATED) {
          this.actual.onError(t);
        }
        this.cancel();
      }
    }
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    long state = this.requested;

    if (key == Attr.TERMINATED) return state == STATE_TERMINATED;
    if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return state;

    return null;
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(RequestStreamFlux)";
  }
}
