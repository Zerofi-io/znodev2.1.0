import { EventEmitter } from 'events';

export class AsyncMutex {
  constructor() {
    this._locked = false;
    this._queue = [];
  }

  async acquire() {
    if (!this._locked) {
      this._locked = true;
      return;
    }

    await new Promise((resolve) => this._queue.push(resolve));
  }

  release() {
    if (this._queue.length > 0) {
      const next = this._queue.shift();
      next();
    } else {
      this._locked = false;
    }
  }

  isLocked() {
    return this._locked;
  }

  async withLock(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

export class ClusterState extends EventEmitter {
  constructor() {
    super();
    this._state = {
      clusterId: null,
      members: [],
      coordinator: null,
      coordinatorIndex: -1,
      finalAddress: null,
      finalized: false,
      pendingR3: null,
      finalizationStartAt: null,
      failoverAttempted: false,
      cooldownUntil: null,
      lastFailureAt: null,
      lastFailureReason: null,
    };
    this._version = 0;
  }

  get state() {
    return { ...this._state };
  }

  get version() {
    return this._version;
  }

  get clusterId() {
    return this._state.clusterId;
  }
  get members() {
    return this._state.members;
  }
  get coordinator() {
    return this._state.coordinator;
  }
  get coordinatorIndex() {
    return this._state.coordinatorIndex;
  }
  get finalAddress() {
    return this._state.finalAddress;
  }
  get finalized() {
    return this._state.finalized;
  }
  get pendingR3() {
    return this._state.pendingR3;
  }
  get finalizationStartAt() {
    return this._state.finalizationStartAt;
  }
  get failoverAttempted() {
    return this._state.failoverAttempted;
  }

  get cooldownUntil() {
    return this._state.cooldownUntil;
  }
  get lastFailureAt() {
    return this._state.lastFailureAt;
  }
  get lastFailureReason() {
    return this._state.lastFailureReason;
  }

  update(changes) {
    const oldState = { ...this._state };
    Object.assign(this._state, changes);
    this._version++;
    this.emit('stateChange', {
      oldState,
      newState: { ...this._state },
      changes,
      version: this._version,
    });
  }

  setCluster(clusterId, members, coordinator, coordinatorIndex) {
    this.update({
      clusterId,
      members: members ? [...members] : [],
      coordinator,
      coordinatorIndex,
      finalizationStartAt: Date.now(),
    });
  }

  setFinalAddress(address) {
    this.update({ finalAddress: address });
  }

  setFinalized() {
    this.update({ finalized: true });
  }

  setPendingR3(payload) {
    this.update({ pendingR3: payload });
  }

  setFailoverAttempted() {
    this.update({ failoverAttempted: true });
  }

  setCooldownUntil(timestamp) {
    this.update({ cooldownUntil: timestamp });
  }

  recordFailure(reason) {
    const now = Date.now();
    this.update({ lastFailureAt: now, lastFailureReason: reason || null });
  }

  clearFailure() {
    this.update({ lastFailureAt: null, lastFailureReason: null });
  }

  hasActiveCluster() {
    return !!(this._state.clusterId && this._state.members && this._state.members.length > 0);
  }

  reset() {
    this.update({
      clusterId: null,
      members: [],
      coordinator: null,
      coordinatorIndex: -1,
      finalAddress: null,
      finalized: false,
      pendingR3: null,
      finalizationStartAt: null,
      failoverAttempted: false,
      cooldownUntil: null,
      lastFailureAt: null,
      lastFailureReason: null,
    });
  }

  clearCluster() {
    this.update({
      clusterId: null,
      members: [],
      coordinator: null,
      coordinatorIndex: -1,
    });
  }
}

export const ClusterStates = {
  IDLE: 'IDLE',
  DISCOVERING: 'DISCOVERING',
  LIVENESS_CHECK: 'LIVENESS_CHECK',
  READY_BARRIER: 'READY_BARRIER',
  ROUND_0: 'ROUND_0',
  MAKE_MULTISIG: 'MAKE_MULTISIG',
  ROUND_3: 'ROUND_3',
  KEY_EXCHANGE: 'KEY_EXCHANGE',
  FINALIZING_MONERO: 'FINALIZING_MONERO',
  FINALIZING_ONCHAIN: 'FINALIZING_ONCHAIN',
  ACTIVE: 'ACTIVE',
  FAILED: 'FAILED',
};

const VALID_TRANSITIONS = {
  [ClusterStates.IDLE]: [ClusterStates.DISCOVERING],
  [ClusterStates.DISCOVERING]: [ClusterStates.LIVENESS_CHECK, ClusterStates.FAILED],
  [ClusterStates.LIVENESS_CHECK]: [ClusterStates.READY_BARRIER, ClusterStates.FAILED],
  [ClusterStates.READY_BARRIER]: [ClusterStates.ROUND_0, ClusterStates.FAILED],
  [ClusterStates.ROUND_0]: [ClusterStates.MAKE_MULTISIG, ClusterStates.FAILED],
  [ClusterStates.MAKE_MULTISIG]: [ClusterStates.ROUND_3, ClusterStates.FAILED],
  [ClusterStates.ROUND_3]: [ClusterStates.KEY_EXCHANGE, ClusterStates.FAILED],
  [ClusterStates.KEY_EXCHANGE]: [ClusterStates.FINALIZING_MONERO, ClusterStates.FAILED],
  [ClusterStates.FINALIZING_MONERO]: [ClusterStates.FINALIZING_ONCHAIN, ClusterStates.FAILED],
  [ClusterStates.FINALIZING_ONCHAIN]: [ClusterStates.ACTIVE, ClusterStates.FAILED],
  [ClusterStates.ACTIVE]: [ClusterStates.IDLE],
  [ClusterStates.FAILED]: [ClusterStates.IDLE],
};

export class ClusterStateMachine extends EventEmitter {
  constructor() {
    super();
    this._currentState = ClusterStates.IDLE;
    this._stateData = {};
    this._stateHistory = [];
    this._stateEnteredAt = Date.now();
    this._maxHistorySize = 100;
  }

  get currentState() {
    return this._currentState;
  }

  get stateData() {
    return { ...this._stateData };
  }

  get timeInState() {
    return Date.now() - this._stateEnteredAt;
  }

  getHistory(limit = 20) {
    return this._stateHistory.slice(-limit);
  }

  canTransitionTo(toState) {
    const validNext = VALID_TRANSITIONS[this._currentState] || [];
    return validNext.includes(toState);
  }

  transition(toState, data = {}, reason = '') {
    if (!this.canTransitionTo(toState)) {
      console.warn(`[StateMachine] Invalid transition: ${this._currentState} -> ${toState}`);
      return false;
    }

    const fromState = this._currentState;
    const timestamp = Date.now();
    const duration = timestamp - this._stateEnteredAt;

    this._stateHistory.push({
      from: fromState,
      to: toState,
      timestamp,
      duration,
      reason,
    });

    if (this._stateHistory.length > this._maxHistorySize) {
      this._stateHistory = this._stateHistory.slice(-this._maxHistorySize);
    }

    this._currentState = toState;
    this._stateData = { ...data };
    this._stateEnteredAt = timestamp;

    this.emit('transition', {
      from: fromState,
      to: toState,
      duration,
      reason,
      data: this._stateData,
    });

    return true;
  }

  fail(reason, error = null) {
    this.transition(
      ClusterStates.FAILED,
      {
        failedFrom: this._currentState,
        reason,
        error: error ? error.message || String(error) : null,
        failedAt: Date.now(),
      },
      reason,
    );
  }

  reset(reason = 'manual reset') {
    const fromState = this._currentState;
    const timestamp = Date.now();

    this._stateHistory.push({
      from: fromState,
      to: ClusterStates.IDLE,
      timestamp,
      duration: timestamp - this._stateEnteredAt,
      reason: `reset: ${reason}`,
    });

    this._currentState = ClusterStates.IDLE;
    this._stateData = {};
    this._stateEnteredAt = timestamp;

    this.emit('reset', { from: fromState, reason });
  }

  isInState(...states) {
    return states.includes(this._currentState);
  }

  isInProgress() {
    return !this.isInState(ClusterStates.IDLE, ClusterStates.ACTIVE, ClusterStates.FAILED);
  }

  getSummary() {
    return {
      state: this._currentState,
      timeInState: this.timeInState,
      data: this._stateData,
      recentHistory: this.getHistory(5),
    };
  }
}

export default {
  AsyncMutex,
  ClusterState,
  ClusterStateMachine,
  ClusterStates,
};
