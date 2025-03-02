package com.purbon.kafka.topology.utils;

import java.util.Optional;

public class Either<L, R> {
  private final L left;
  private final R right;

  private Either(L left, R right) {
    this.left = left;
    this.right = right;
  }

  public static <L, R> Either<L, R> Left(L value) {
    return new Either(value, null);
  }

  public static <L, R> Either<L, R> Right(R value) {
    return new Either(null, value);
  }

  public Optional<L> getLeft() {
    return Optional.ofNullable(left);
  }

  public Optional<R> getRight() {
    return Optional.ofNullable(right);
  }

  public boolean isLeft() {
    return left != null;
  }

  public boolean isRight() {
    return right != null;
  }

  public String toString() {
    if (isLeft()) {
      return "Left(" + left + ")";
    }
    return "Right(" + right + ")";
  }
}
