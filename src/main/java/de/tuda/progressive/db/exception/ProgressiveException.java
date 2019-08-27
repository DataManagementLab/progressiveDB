package de.tuda.progressive.db.exception;

public class ProgressiveException extends RuntimeException {

  public ProgressiveException() {}

  public ProgressiveException(String message) {
    super(message);
  }

  public ProgressiveException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProgressiveException(Throwable cause) {
    super(cause);
  }

  public ProgressiveException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
