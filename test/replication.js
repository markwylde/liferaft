const assume = require('assume');
const Raft = require('../');
const Log = require('../log');
const net = require('net');
const util = require('util');
const { rimraf } = require('rimraf');
const { mkdirp } = require('mkdirp');
const debug = require('diagnostics')('cluster');
const port = 8088;

/* istanbul ignore next */
describe('liferaft Log Replication', () => {
  class WoodenRaft extends Raft {
    /**
     * Initialize the server so we can receive connections.
     *
     * @param {Object} options Received optiosn when constructing the client.
     * @api private
     */
     initialize(options) {
      const raft = this;
      this.write = this.write.bind(this);
      const sockets = [];
      const server = net.createServer((socket) => {
        socket.on('data', (buff) => {
          const data = JSON.parse(buff.toString());

          this.emit('data', data, (data) => {
            if (socket.destroyed) {
              socket.end();
              return;
            }
            socket.write(JSON.stringify(data));
            socket.end();
          });
        });
      }).listen(this.address);

      server.on('connection', socket => {
        sockets.push(socket);
      });
      raft.server = server;

      this.once('end', () => {
        sockets.forEach(s => s.destroy());
        server.close();
      });
    }

    /**
     * Write to the connection.
     *
     * @param {Object} packet Data to be transfered.
     * @param {Function} fn Completion callback.
     * @api public
     */
    write(packet, fn) {
      const socket = net.connect(this.address);
      const raft = this;

      debug(this.address +':packet#write', packet);
      socket.on('error', fn);
      socket.on('data', (buff) => {
        let data;

        try { data = JSON.parse(buff.toString()); }
        catch (e) { return fn(e); }

        debug(raft.address +':packet#callback', packet);
        fn(undefined, data);
      });

      socket.setNoDelay(true);
      socket.write(JSON.stringify(packet));
    }
  }

  let node1, node2, node3;

  beforeEach(async () => {
    if (process.env.ADAPTER === 'leveldown') {
      const leveldbPath = `${process.cwd()}/tmp`;
      await rimraf(leveldbPath);
      await mkdirp(leveldbPath);
      node1 = new WoodenRaft({address: 8111, Log, path: './tmp/8111'});
      node2 = new WoodenRaft({address: 8112, Log, path: './tmp/8112'});
    } else {
      node2 = new WoodenRaft({address: 8112, Log, adapter: require('memdown')});
      node1 = new WoodenRaft({address: 8111, Log, adapter: require('memdown')});
    }
  });

  afterEach((next) => {
    const promises = [node3, node2, node1].map(node => {
      if (!node) {
        return Promise.resolve();
      }
      return new Promise((resolve) => {
        node.once('end', () => {
          resolve();
        });
        node.end();
      });
    });

    Promise.all(promises).then((args) => {
      node1 = node2 = node3 = false;
      next();
    });
  });

  it('Sends correct index with start log', (next) => {

    node1.join(8112);
    node2.join(8111);

    node2.on('data', (packet, write) => {
      if (packet.type === 'append') {
        assume(packet.last.committedIndex).equals(0);
        assume(packet.last.index).equals(0);
        next();
      }
    });

    node1.promote();
  });

  it('sends append to nodes with command', (next) => {
    const command1 = {first: 'command'};
    node1.join(8112);
    node2.join(8111);

    node2.on('data', (packet, write) => {
      if (packet.type === 'append' && packet.data) {
        assume(packet.last.index).equals(0);
        assume(packet.last.committedIndex).equals(0);
        assume(packet.data[0].command).deep.equals(command1);
        //not pretty but just need a small delay otherwise it shuts down the logs
        // and node before the command has process and causes the test to throw an exception
        setTimeout(next, 10);
      }
    });

    node1.once('leader', async () => {
      await node1.command(command1);
    });

    node1.promote();
  });

  it('commits command once a quorum has been reached', (next) => {
    const command1 = {first: 'command'};
    node1.join(8112);
    node2.join(8111);

    node1.once('commit', (command) => {
      assume(command).deep.equals(command1);
      next();
    });

    node1.once('leader', async () => {
      await node1.command(command1);
    });

    node1.promote();
  });

  it('does not commit command if same node ack', async () => {
    const command1 = {first: 'command'};
    node1.state = Raft.LEADER;
    await node1.command(command1);

    await node1.log.commandAck(1, 8111);
    const entry = await node1.log.get(1);
    assume(entry.responses.length).deep.equals(1);
  });

  it('follow commits command on next hearbeat', (next) => {
    const command1 = {first: 'command'};
    node1.join(8112);
    node2.join(8111);

    node1.once('commit', (command) => {
      node2.once('commit', (command) => {
        assume(command).deep.equals(command1);
        next();
      });
    });

    node1.once('leader', async () => {
      node1.command(command1);
    });

    node1.promote();
  });

  it('commits second command correctly', (next) => {
    const command1 = {first: 'command'};
    const command2 = {second: 'command2'};
    let commitCalledOnNode1 = false;
    node1.join(8112);
    node2.join(8111);

    node1.once('commit', (command) => {
      assume(command).deep.equals(command1);

      node2.once('commit', (command) => {
        assume(command).deep.equals(command1);

        node1.once('commit', (command) => {
          assume(command).deep.equals(command2);
          commitCalledOnNode1 = true;
        });

        node2.once('commit', (command) => {
          assume(command).deep.equals(command2);
          assume(commitCalledOnNode1).be.true;
          next();
        });

        node2.on('data', (packet, write) => {
          if (packet.type === 'append' && packet.data) {
            assume(packet.last.index).equals(1);
            assume(packet.last.committedIndex).equals(1);
            assume(packet.data[0].command).deep.equals(command2);
          }
        });

        node1.command(command2);
      });
    });

    node1.once('leader', () => {
      node1.command(command1);
    });
    node1.promote();
  });

  it('replicates log to node', (next) => {
    const command1 = {first: 'command'};
    const command2 = {second: 'command2'};

    node1.join(8112);
    node2.join(8111);

    node1.once('leader', async () => {
      await node1.command(command1);
      await node1.command(command2);
    });

    node2.once('commit', (command) => {
      assume(command).deep.equals(command1);
      node2.once('commit', (command) => {
        assume(command).deep.equals(command2);
        next();
      });
    });

    node1.promote();
  });

  it('Replicates log to new node', (next) => {
    const command1 = {first: 'command'};
    const command2 = {second: 'command2'};
    node1.join(8112);
    node2.join(8111);

    node2.once('commit', (command) => {
      assume(command).deep.equals(command1);
      node1.command(command2);

      node2.once('commit', (command) => {
        assume(command).deep.equals(command2);

        if (process.env.ADAPTER === 'leveldown') {
          node3 = new WoodenRaft({address: 8113, Log, path: './tmp/8113'});
        } else {
          node3 = new WoodenRaft({address: 8113, Log, adapter: require('memdown')});
        }

        node3.join(8111);
        node3.join(8112);

        node1.join(8113);
        node2.join(8113);

        node3.once('commit', (command) => {
          assume(command).deep.equals(command1);

          node3.once('commit', (command) => {
            assume(command).deep.equals(command2);
            next();
          });
        });
      });
    });

    node1.once('leader', () => {
      node1.command(command1);
    });
    node1.promote();
  });

  it('brings cluster up to date', (next) => {
    const command1 = { first: 'command' };
    const command2 = { second: 'command2' };
    const commandNotCommitted1 = { wrong: 'command' };
    const commandNotCommitted2 = { wrong: 'command2' };
    // node1 = new WoodenRaft({address: 8111, Log, adapter: require('memdown')});
    // node2 = new WoodenRaft({address: 8112, Log, adapter: require('memdown')});
    // node1 = new WoodenRaft({address: 8111, Log, path: './tmp/8111'});
    // node2 = new WoodenRaft({address: 8112, Log, path: './tmp/8112'});

    // add log entries
    node1.log.put({
      term: 1,
      index: 1,
      committed: true,
      responses: [{ address: 8111, ack: true }, { address: 8000, ack: true }],
      command: command1,
    });

    node2.log.put({
      term: 1,
      index: 1,
      committed: true,
      responses: [{ address: 8111, ack: true }, { address: 8000, ack: true }],
      command: command1,
    });

    node2.log.put({
      term: 1,
      index: 2,
      committed: false,
      responses: [{ address: 8112, ack: true }],
      command: commandNotCommitted1,
    });

    node2.log.put({
      term: 1,
      index: 3,
      committed: false,
      responses: [{ address: 8112, ack: true }],
      command: commandNotCommitted2,
    });

    node1.log.committedIndex = 1;
    node2.log.committedIndex = 1;

    node1.term = 2;
    node2.term = 1;
    node1.join(8112);
    node2.join(8111);

    node2.once('leader', () => {
      node2.command(command2);

      let timesChecked = 0;
      async function checkNodeCommit(command) {
        try {
          let newCommand;
          if (timesChecked === 0) newCommand = commandNotCommitted1;
          if (timesChecked === 1) newCommand = commandNotCommitted2;
          if (timesChecked === 2) newCommand = command2;
          timesChecked++;
          assume(command).deep.equals(newCommand);
          assume(node1.log.committedIndex).equals(1 + timesChecked);

          if (timesChecked === 3) {
            next();
          }
        } catch (ex) {
          next(ex);
        }
      }

      node1.on('commit', checkNodeCommit);
      node1.once('leader', () =>  next(new Error('Node1 should not have become leader')));
      node1.promote(); // Try to promote node1 but it should not become leader as it has a lower written index
    });
  });

});
