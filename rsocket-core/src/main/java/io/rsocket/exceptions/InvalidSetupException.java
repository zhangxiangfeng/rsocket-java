/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.exceptions;

import io.rsocket.frame.ErrorFrameFlyweight;
import javax.annotation.Nullable;

/**
 * The Setup frame is invalid for the server (it could be that the client is too recent for the old
 * server).
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-codes">Error
 *     Codes</a>
 */
public final class InvalidSetupException extends SetupException {

  private static final long serialVersionUID = -6816210006610385251L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param message the message
   */
  public InvalidSetupException(String message) {
    this(message, null);
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param message the message
   * @param cause the cause of this exception
   */
  public InvalidSetupException(String message, @Nullable Throwable cause) {
    super(ErrorFrameFlyweight.INVALID_SETUP, message, cause);
  }
}
