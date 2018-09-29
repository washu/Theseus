package ai.eloquent.monitoring;

import org.junit.Test;

import static org.junit.Assert.*;

public class PrometheusTest {


  @Test
  public void observeDuration() {
    Object METRIC_TIMING = Prometheus.summaryBuild("udp_transport", "Statistics on the UDP Transport calls", "operation");
    assertNotNull(METRIC_TIMING);
    Object prometheusBegin = Prometheus.startTimer(METRIC_TIMING, "parse_upd_packet");
    assertNotNull(prometheusBegin);
    Object prometheusEnd = Prometheus.observeDuration(prometheusBegin);
    assertNotNull(prometheusEnd);
  }
}