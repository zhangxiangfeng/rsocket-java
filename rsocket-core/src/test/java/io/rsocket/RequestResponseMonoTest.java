package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

import static io.rsocket.RequestResponseMono.FLAG_REASSEMBLING;
import static io.rsocket.RequestResponseMono.FLAG_SENT;
import static io.rsocket.RequestResponseMono.STATE_REQUESTED;
import static io.rsocket.RequestResponseMono.STATE_TERMINATED;
import static io.rsocket.RequestResponseMono.STATE_UNSUBSCRIBED;

public class RequestResponseMonoTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /*
   * +-------------------------------+
   * |      General Test Cases       |
   * +-------------------------------+
   *
   */

  /**
   * General StateMachine transition test.
   * No Fragmentation enabled
   * In this test we check that the given instance of RequestResponseMono:
   *  1) subscribes
   *  2) sends frame on the first request
   *  3) terminates up on receiving the first signal (terminates on first next | error | next over reassembly | complete)
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameShouldBeSentOnSubscription(
      BiFunction<RequestResponseMono, StepVerifier.Step<Payload>, StepVerifier> transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(requestResponseMono.state)
            .isEqualTo(RequestResponseMono.STATE_UNSUBSCRIBED);
    Assertions.assertThat(activeStreams).isEmpty();

    transformer
        .apply(
            requestResponseMono,
            StepVerifier.create(requestResponseMono, 0)
                .expectSubscription()
                .then(() ->
                        Assertions.assertThat(requestResponseMono.state)
                                .isEqualTo(RequestResponseMono.STATE_SUBSCRIBED)
                )
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> Assertions.assertThat(activeStreams).isEmpty())
                .thenRequest(1)
                .then(() ->
                        Assertions.assertThat(requestResponseMono.state)
                                .isEqualTo(STATE_REQUESTED | FLAG_SENT)
                )
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(
                    () ->
                        Assertions.assertThat(activeStreams).containsEntry(1, requestResponseMono)))
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();
    // should not add anything to map
    Assertions.assertThat(activeStreams).isEmpty();

    final ByteBuf frame = sender.poll();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();
    Assertions.assertThat(requestResponseMono.state)
            .isEqualTo(STATE_TERMINATED);
    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.poll())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);
    }
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<BiFunction<RequestResponseMono, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnSubscriptionResponses() {
    return Stream.of(
        // next case
        (rrm, sv) ->
            sv.then(() -> rrm.onNext(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .expectComplete(),
        // complete case
        (rrm, sv) -> sv.then(rrm::onComplete).expectComplete(),
        // error case
        (rrm, sv) ->
            sv.then(() -> rrm.onError(new ApplicationErrorException("test")))
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        // fragmentation case
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);

          return sv.then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFirstFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            FrameType.REQUEST_RESPONSE,
                            1,
                            payload.metadata(),
                            payload.data());
                    rrm.reassemble(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(() ->
                    Assertions.assertThat(rrm.state)
                            .isEqualTo(STATE_REQUESTED | FLAG_SENT | FLAG_REASSEMBLING)
              )
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rrm.reassemble(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(() ->
                      Assertions.assertThat(rrm.state)
                              .isEqualTo(STATE_REQUESTED | FLAG_SENT | FLAG_REASSEMBLING)
              )
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rrm.reassemble(followingFrame, true, false);
                    followingFrame.release();
                  })

              .then(() ->
                      Assertions.assertThat(rrm.state)
                              .isEqualTo(STATE_REQUESTED | FLAG_SENT | FLAG_REASSEMBLING)
              )
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rrm.reassemble(followingFrame, false, false);
                    followingFrame.release();
                  })

              .then(() ->
                      Assertions.assertThat(rrm.state)
                              .isEqualTo(STATE_TERMINATED)
              )
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    p.release();
                  })
              .then(payload::release)
              .expectComplete();
        },
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);

          ByteBuf[] fragments =
              new ByteBuf[] {
                FragmentationUtils.encodeFirstFragment(
                    ByteBufAllocator.DEFAULT,
                    64,
                    FrameType.REQUEST_RESPONSE,
                    1,
                    payload.metadata(),
                    payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    ByteBufAllocator.DEFAULT, 64, 1, false, payload.metadata(), payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    ByteBufAllocator.DEFAULT, 64, 1, false, payload.metadata(), payload.data())
              };

          final StepVerifier stepVerifier =
              sv.then(
                      () -> {
                        rrm.reassemble(fragments[0], true, false);
                        fragments[0].release();
                      })
                      .then(() ->
                              Assertions.assertThat(rrm.state)
                                      .isEqualTo(STATE_REQUESTED | FLAG_SENT | FLAG_REASSEMBLING)
                      )
                  .then(
                      () -> {
                        rrm.reassemble(fragments[1], true, false);
                        fragments[1].release();
                      })
                      .then(() ->
                              Assertions.assertThat(rrm.state)
                                      .isEqualTo(STATE_REQUESTED | FLAG_SENT | FLAG_REASSEMBLING)
                      )
                  .then(
                      () -> {
                        rrm.reassemble(fragments[2], true, false);
                        fragments[2].release();
                      })
                      .then(() ->
                              Assertions.assertThat(rrm.state)
                                      .isEqualTo(STATE_REQUESTED | FLAG_SENT | FLAG_REASSEMBLING)
                      )
                  .then(payload::release)
                  .thenCancel()
                  .verifyLater();

          stepVerifier.verify();

          Assertions.assertThat(fragments).allMatch(bb -> bb.refCnt() == 0);

          return stepVerifier;
        });
  }

  /**
   * General StateMachine transition test.
   * Fragmentation enabled
   * In this test we check that the given instance of RequestResponseMono:
   *  1) subscribes
   *  2) sends fragments frames on the first request
   *  3) terminates up on receiving the first signal (terminates on first next | error | next over reassembly | complete)
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameFragmentsShouldBeSentOnSubscription(
      BiFunction<RequestResponseMono, StepVerifier.Step<Payload>, StepVerifier> transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            64,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);


    Assertions.assertThat(requestResponseMono.state)
            .isEqualTo(RequestResponseMono.STATE_UNSUBSCRIBED);
    Assertions.assertThat(activeStreams).isEmpty();

    transformer
        .apply(
            requestResponseMono,
            StepVerifier.create(requestResponseMono, 0)
                .expectSubscription()
                .then(() ->
                        Assertions.assertThat(requestResponseMono.state)
                                .isEqualTo(RequestResponseMono.STATE_SUBSCRIBED)
                )
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> Assertions.assertThat(activeStreams).isEmpty())
                .thenRequest(1)
                .then(() ->
                        Assertions.assertThat(requestResponseMono.state)
                                .isEqualTo(STATE_REQUESTED | FLAG_SENT)
                )
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(
                    () ->
                        Assertions.assertThat(activeStreams).containsEntry(1, requestResponseMono)))
        .verify();

    // should not add anything to map
    Assertions.assertThat(activeStreams).isEmpty();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.poll();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(55) // 64 - 3 (frame headers) - 3 (encoded metadata length) - 3 frame length
        .hasMetadata(Arrays.copyOf(metadata, 55))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment1.release()).isTrue();
    Assertions.assertThat(frameFragment1.refCnt()).isZero();

    final ByteBuf frameFragment2 = sender.poll();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(55) // 64 - 3 (frame headers) - 3 (encoded metadata length) - 3 frame length
        .hasMetadata(Arrays.copyOfRange(metadata, 55, 65))
        .hasData(Arrays.copyOf(data, 45))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment2.release()).isTrue();
    Assertions.assertThat(frameFragment2.refCnt()).isZero();

    final ByteBuf frameFragment3 = sender.poll();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(58) // 64 - 3 (frame headers) - 3 frame length (no metadata - no length)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 45, 103))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment3.release()).isTrue();
    Assertions.assertThat(frameFragment3.refCnt()).isZero();

    final ByteBuf frameFragment4 = sender.poll();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(26)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 103, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment4.release()).isTrue();
    Assertions.assertThat(frameFragment4.refCnt()).isZero();
    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.poll())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);
    }
    Assertions.assertThat(sender).isEmpty();
    Assertions.assertThat(requestResponseMono.state)
            .isEqualTo(STATE_TERMINATED);
  }

  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<RequestResponseMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(requestResponseMono.state)
            .isEqualTo(STATE_UNSUBSCRIBED);
    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestResponseMono);

    Assertions.assertThat(requestResponseMono.state)
            .isEqualTo(STATE_TERMINATED);
    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender).isEmpty();
  }

  static Stream<Consumer<RequestResponseMono>> shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestResponseMono ->
            Assertions.assertThatThrownBy(requestResponseMono::block)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhase() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("");

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            RequestStreamFluxTest.TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    StepVerifier.create(requestResponseMono, 0)
        .expectSubscription()
        .then(payload::release)
        .thenRequest(1)
        .expectError(IllegalReferenceCountException.class)
        .verify();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhaseWithFragmentation() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            64,
            RequestStreamFluxTest.TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    StepVerifier.create(requestResponseMono, 0)
        .expectSubscription()
        .then(payload::release)
        .thenRequest(1)
        .expectError(IllegalReferenceCountException.class)
        .verify();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestResponseMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    final byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestResponseMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender).isEmpty();
  }

  static Stream<Consumer<RequestResponseMono>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("Too Big Payload size")
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        requestResponseMono ->
            Assertions.assertThatThrownBy(requestResponseMono::block)
                .hasMessage("Too Big Payload size")
                .isInstanceOf(IllegalArgumentException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestResponseMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.error(new RuntimeException("test")),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestResponseMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
  }

  static Stream<Consumer<RequestResponseMono>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        requestResponseMono ->
            Assertions.assertThatThrownBy(requestResponseMono::block)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  @Test
  public void shouldSubscribeExactlyOnce1() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 1000; i++) {
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () ->
                          requestResponseMono.subscribe(
                              null,
                              t -> {
                                throw Exceptions.propagate(t);
                              }),
                      () ->
                          requestResponseMono.subscribe(
                              null,
                              t -> {
                                throw Exceptions.propagate(t);
                              })))
          .matches(
              t -> {
                if (t instanceof IllegalReferenceCountException) {
                  Assertions.assertThat(t).hasMessage("refCnt: 0");
                } else {
                  Assertions.assertThat(t)
                      .hasMessage("RequestResponseMono allows only a single Subscriber");
                }
                return true;
              });

      final ByteBuf frame = sender.poll();
      FrameAssert.assertThat(frame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(frame.release()).isTrue();
      Assertions.assertThat(frame.refCnt()).isZero();
    }

    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldBeNoOpsOnCancel() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    StepVerifier.create(requestResponseMono, 0)
        .expectSubscription()
        .then(() -> Assertions.assertThat(activeStreams).isEmpty())
        .thenCancel()
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldSentRequestResponseFrameOnceInCaseOfRequestRacing() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseMono.doOnNext(Payload::release).subscribeWith(AssertSubscriber.create(0));

      RaceTestUtils.race(() -> assertSubscriber.request(1), () -> assertSubscriber.request(1));

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      requestResponseMono.onNext(response);

      assertSubscriber.assertTerminated();

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      ByteBuf frame = Unpooled.wrappedBuffer("test".getBytes(CharsetUtil.UTF_8));

      StepVerifier.create(requestResponseMono).expectSubscription().expectComplete().verifyLater();

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(
          requestResponseMono::cancel,
          () -> {
            requestResponseMono.reassemble(frame, true, false);
            frame.release();
          });

      final ByteBuf cancellationFrame = sender.poll();
      FrameAssert.assertThat(cancellationFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(frame.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      Assertions.assertThat(sender.isEmpty()).isTrue();

      Assertions.assertThat(requestResponseMono.state)
          .isEqualTo(RequestResponseMono.STATE_TERMINATED);
    }
  }

  @Test
  public void shouldHaveNoLeaksOnNextAndCancelRacing() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      StepVerifier.create(requestResponseMono.doOnNext(Payload::release))
          .expectSubscription()
          .expectComplete()
          .verifyLater();

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(requestResponseMono::cancel, () -> requestResponseMono.onNext(response));

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      final boolean isEmpty = sender.isEmpty();
      if (!isEmpty) {
        final ByteBuf cancellationFrame = sender.poll();
        FrameAssert.assertThat(cancellationFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1);
      }
      Assertions.assertThat(sender.isEmpty()).isTrue();

      Assertions.assertThat(requestResponseMono.state)
          .isEqualTo(RequestResponseMono.STATE_TERMINATED);
    }
  }

  @Test
  public void shouldBeConsistentInCaseOfRacingOfCancellationAndRequest() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              RequestStreamFluxTest.TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseMono.subscribeWith(new AssertSubscriber<>(0));

      RaceTestUtils.race(() -> assertSubscriber.cancel(), () -> assertSubscriber.request(1));

      if (!sender.isEmpty()) {
        final ByteBuf sentFrame = sender.poll();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_RESPONSE)
            .hasPayloadSize(
                "testData".getBytes(CharsetUtil.UTF_8).length
                    + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
            .hasMetadata("testMetadata")
            .hasData("testData")
            .hasNoFragmentsFollow()
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(sentFrame.release()).isTrue();
        Assertions.assertThat(sentFrame.refCnt()).isZero();

        final ByteBuf cancelFrame = sender.poll();
        FrameAssert.assertThat(cancelFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1);

        Assertions.assertThat(cancelFrame.release()).isTrue();
        Assertions.assertThat(cancelFrame.refCnt()).isZero();
      }

      Assertions.assertThat(payload.refCnt()).isZero();

      Assertions.assertThat(requestResponseMono.state)
          .isEqualTo(RequestResponseMono.STATE_TERMINATED);

      requestResponseMono.onNext(response);
      Assertions.assertThat(response.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void shouldSentCancelFrameExactlyOnce() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              RequestStreamFluxTest.TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseMono.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(requestResponseMono::cancel, requestResponseMono::cancel);

      final ByteBuf cancelFrame = sender.poll();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(cancelFrame.release()).isTrue();
      Assertions.assertThat(cancelFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(activeStreams).isEmpty();

      Assertions.assertThat(requestResponseMono.state)
          .isEqualTo(RequestResponseMono.STATE_TERMINATED);

      requestResponseMono.onNext(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestResponseMono.onComplete();
      assertSubscriber.assertNotTerminated();

      Assertions.assertThat(activeStreams).isEmpty();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void checkName() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(Scannable.from(requestResponseMono).name())
        .isEqualTo("source(RequestResponseMono)");
  }

  static final class TestStateAware implements StateAware {

    final Throwable error;

    TestStateAware(Throwable error) {
      this.error = error;
    }

    @Override
    public Throwable error() {
      return error;
    }

    @Override
    public Throwable checkAvailable() {
      return error;
    }

    public static TestStateAware error(Throwable e) {
      return new TestStateAware(e);
    }

    public static TestStateAware empty() {
      return new TestStateAware(null);
    }
  }
}
