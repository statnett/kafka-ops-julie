package com.purbon.kafka.topology.validation.topic;

import com.purbon.kafka.topology.exceptions.ValidationException;

final class TopicValidationUtils {

  private TopicValidationUtils() {}

  public static boolean compare(final int value1, final String op, final int value2)
      throws ValidationException {
    return switch (op) {
      case "gt" -> value1 > value2;
      case "lt" -> value1 < value2;
      case "eq" -> value1 == value2;
      case "gte" -> value1 >= value2;
      case "lte" -> value1 <= value2;
      case "ne" -> value1 != value2;
      default -> throw new ValidationException("Invalid Operation code in use " + op);
    };
  }
}
