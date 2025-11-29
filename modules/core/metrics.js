import { EventEmitter } from 'events';

export class ClusterMetrics extends EventEmitter {
  constructor() {
    super();
    this.counters = {
      roundsStarted: 0,
      roundsCompleted: 0,
      roundsFailed: 0,
      clustersJoined: 0,
      clustersFinalized: 0,
      clustersFailed: 0,
      messagesReceived: 0,
      messagesSent: 0,
      walletRotations: 0,
      rpcErrors: 0,
    };
    this.gauges = {
      activeClusterMembers: 0,
      currentRound: null,
      currentState: 'IDLE',
      lastHeartbeatAge: 0,
      connectedPeers: 0,
    };
    this.histograms = {
      roundDurationMs: [],
      clusterFormationMs: [],
    };
    this._startTimes = {};
  }

  increment(counter, amount = 1) {
    if (this.counters.hasOwnProperty(counter)) {
      this.counters[counter] += amount;
      this.emit('counterUpdated', { counter, value: this.counters[counter] });
    }
  }

  setGauge(gauge, value) {
    if (this.gauges.hasOwnProperty(gauge)) {
      this.gauges[gauge] = value;
      this.emit('gaugeUpdated', { gauge, value });
    }
  }

  recordHistogram(histogram, value) {
    if (this.histograms.hasOwnProperty(histogram)) {
      this.histograms[histogram].push(value);

      if (this.histograms[histogram].length > 100) {
        this.histograms[histogram].shift();
      }
      this.emit('histogramUpdated', { histogram, value });
    }
  }

  startTimer(key) {
    this._startTimes[key] = Date.now();
  }

  stopTimer(key, histogram) {
    if (this._startTimes[key]) {
      const duration = Date.now() - this._startTimes[key];
      delete this._startTimes[key];
      if (histogram) {
        this.recordHistogram(histogram, duration);
      }
      return duration;
    }
    return null;
  }

  roundStarted(round) {
    this.increment('roundsStarted');
    this.setGauge('currentRound', round);
    this.startTimer(`round_${round}`);
  }

  roundCompleted(round) {
    this.increment('roundsCompleted');
    const duration = this.stopTimer(`round_${round}`, 'roundDurationMs');
    return duration;
  }

  roundFailed(round) {
    this.increment('roundsFailed');
    this.stopTimer(`round_${round}`);
  }

  clusterStarted(clusterId) {
    this.increment('clustersJoined');
    this.startTimer(`cluster_${clusterId}`);
  }

  clusterFinalized(clusterId) {
    this.increment('clustersFinalized');
    const duration = this.stopTimer(`cluster_${clusterId}`, 'clusterFormationMs');
    return duration;
  }

  clusterFailed(clusterId) {
    this.increment('clustersFailed');
    this.stopTimer(`cluster_${clusterId}`);
  }

  onStateTransition(from, to, data) {
    this.setGauge('currentState', to);
    if (data && data.memberCount) {
      this.setGauge('activeClusterMembers', data.memberCount);
    }
    if (data && data.round !== undefined) {
      this.setGauge('currentRound', data.round);
    }
  }

  getHistogramStats(histogram) {
    const values = this.histograms[histogram] || [];
    if (values.length === 0) {
      return { count: 0, min: null, max: null, avg: null, p50: null, p95: null };
    }
    const sorted = [...values].sort((a, b) => a - b);
    const sum = values.reduce((a, b) => a + b, 0);
    return {
      count: values.length,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      avg: Math.round(sum / values.length),
      p50: sorted[Math.floor(sorted.length * 0.5)],
      p95: sorted[Math.floor(sorted.length * 0.95)],
    };
  }

  toJSON() {
    return {
      counters: { ...this.counters },
      gauges: { ...this.gauges },
      histograms: {
        roundDurationMs: this.getHistogramStats('roundDurationMs'),
        clusterFormationMs: this.getHistogramStats('clusterFormationMs'),
      },
      timestamp: Date.now(),
    };
  }

  reset() {
    Object.keys(this.counters).forEach((k) => (this.counters[k] = 0));
    Object.keys(this.gauges).forEach(
      (k) => (this.gauges[k] = k === 'currentState' ? 'IDLE' : k === 'currentRound' ? null : 0),
    );
    Object.keys(this.histograms).forEach((k) => (this.histograms[k] = []));
    this._startTimes = {};
    this.emit('reset');
  }
}

export const metrics = new ClusterMetrics();
export default ClusterMetrics;
