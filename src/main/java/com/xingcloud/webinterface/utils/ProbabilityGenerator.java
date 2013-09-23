package com.xingcloud.webinterface.utils;

import java.util.Random;

public class ProbabilityGenerator {
  public static boolean doWithProbability(double probability) {
    return new Random().nextDouble() < probability;
  }

  public static void main(String[] args) {
    for (int i = 0; i < 100; i++) {
      System.out.println(doWithProbability(1.0 / 3));
    }
  }
}
