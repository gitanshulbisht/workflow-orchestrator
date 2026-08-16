package com.buildathon.orchestrator.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffCalculatorTest {

    @Test
    void maxDelayGrowsExponentially() {
        assertThat(BackoffCalculator.maxDelayMillis(5, 2.0, 1)).isEqualTo(5000);
        assertThat(BackoffCalculator.maxDelayMillis(5, 2.0, 2)).isEqualTo(10000);
        assertThat(BackoffCalculator.maxDelayMillis(5, 2.0, 3)).isEqualTo(20000);
        assertThat(BackoffCalculator.maxDelayMillis(5, 2.0, 4)).isEqualTo(40000);
    }

    @Test
    void backoffFactorOfOneIsConstant() {
        assertThat(BackoffCalculator.maxDelayMillis(10, 1.0, 1)).isEqualTo(10000);
        assertThat(BackoffCalculator.maxDelayMillis(10, 1.0, 5)).isEqualTo(10000);
    }

    @Test
    void jitteredDelayStaysWithinBounds() {
        for (int attempt = 1; attempt <= 8; attempt++) {
            long max = BackoffCalculator.maxDelayMillis(5, 2.0, attempt);
            for (int i = 0; i < 50; i++) {
                long delay = BackoffCalculator.computeDelayMillis(5, 2.0, attempt);
                assertThat(delay).isBetween(0L, max);
            }
        }
    }

    @Test
    void rejectsInvalidAttemptNumbers() {
        assertThatThrownBy(() -> BackoffCalculator.computeDelayMillis(5, 2.0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BackoffCalculator.computeDelayMillis(5, 2.0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
