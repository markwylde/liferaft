const debug = require('diagnostics')('raft');
const argv = require('argh').argv;
const LifeRaft = require('../');

let msg;

if (argv.queue) {
  msg = require(argv.queue);
} else {
  msg = require('axon');
}

class MsgRaft extends LifeRaft {
  initialize (options) {
    debug('initializing reply socket on port %s', this.address);

    const socket = this.socket = msg.socket('rep');

    socket.bind(this.address);
    socket.on('message', (data, callback) => {
      this.emit('data', data, callback);
    });

    socket.on('error', () => {
      debug('failed to initialize on port: ', this.address);
    });
  }

  write (packet, callback) {
    if (!this.socket) {
      this.socket = msg.socket('req');

      this.socket.connect(this.address);
      this.socket.on('error', function err () {
        console.error('failed to write to: ', this.address);
      });
    }

    debug('writing packet to socket on port %s', this.address);
    this.socket.send(packet, (data) => {
      callback(undefined, data);
    });
  }
}

const ports = [
  8081
];

const port = +argv.port || ports[0];

const raft = new MsgRaft('tcp://127.0.0.1:' + port, {
  'election min': 2000,
  'election max': 5000,
  heartbeat: 1000
});

raft.on('heartbeat timeout', function () {
  debug('heart beat timeout, starting election');
});

raft.on('term change', function (to, from) {
  debug('were now running on term %s -- was %s', to, from);
});

raft.on('leader change', function (to, from) {
  debug('we have a new leader to: %s -- was %s', to, from);
});

raft.on('state change', function (to, from) {
  debug('we have a state to: %s -- was %s', to, from);
});

raft.on('leader', function () {
  console.log('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@');
  console.log('I am elected as leader');
  console.log('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@');
});

raft.on('candidate', function () {
  console.log('----------------------------------');
  console.log('I am starting as candidate');
  console.log('----------------------------------');
});

ports.forEach((nr) => {
  if (!nr || port === nr) {
    return;
  }

  raft.join('tcp://127.0.0.1:' + nr);
});
