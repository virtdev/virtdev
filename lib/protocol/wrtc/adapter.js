/*      adapter.js
 *
 *      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
 *
 *      This program is free software; you can redistribute it and/or modify
 *      it under the terms of the GNU General Public License as published by
 *      the Free Software Foundation; either version 2 of the License, or
 *      (at your option) any later version.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU General Public License for more details.
 *
 *      You should have received a copy of the GNU General Public License
 *      along with this program; if not, write to the Free Software
 *      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 *      MA 02110-1301, USA.
 */

var ws = require('ws');
var mqtt = require('mqtt');
var wrtc = require('wrtc');
var crypto = require("crypto");
var WebSocketClient = require('websocket').client;
var argv = require('minimist')(process.argv.slice(2));

var RTCIceCandidate = wrtc.RTCIceCandidate;
var RTCPeerConnection = wrtc.RTCPeerConnection;
var RTCSessionDescription = wrtc.RTCSessionDescription;

var dataChannels = {};
var peerConnections = {};

var nodeKey = argv.k;
var bridgeAddr = argv.b;
var bridgePort = argv.p;
var nodeAddress = argv.a;
var conductorPort = argv.c;
var cryptkey = nodeKey.substring(0, 16);
var cryptiv = nodeKey.substring(16, 32);
var server = new ws.Server({'port': argv.l});
var listener = mqtt.connect('mqtt://' + bridgeAddr + ':' + bridgePort);

function encrypt(args, key)
{
  var k = key.substring(0, 16);
  var iv = key.substring(16, 32);
  var data = JSON.stringify(args);
  var encipher = crypto.createCipheriv('aes-128-cbc', k, iv);
  var enc = encipher.update(data, 'utf8', 'binary');

  enc += encipher.final('binary');
  return enc;
}

function decrypt(data)
{
  var decipher = crypto.createDecipheriv('aes-128-cbc', cryptkey, cryptiv);
  var dec = decipher.update(data, 'binary', 'utf8');

  dec += decipher.final('utf8');
  return dec;
}

function connect(addr, key, bridge)
{
  if (addr in peerConnections)
    return;

  var pc = new RTCPeerConnection(
    {
      iceServers: [{url:'stun:23.31.150.121'}]
    },
    {
      'optional': []
    }
  );

  pc.onicecandidate = function(event) {
    var candidate = event.candidate;

    if (!candidate)
      return;

    var client = mqtt.connect('mqtt://' + bridge + ':' + bridgePort);

    client.on('connect', function () {
      var args = {};

      args.cmd = 'candidate';
      args.cand = candidate;
      args.addr = nodeAddress;
      client.publish(addr, encrypt(args, key));
      client.end();
    });
  };

  var channel = pc.createDataChannel('reliable', {
      ordered: false,
      maxRetransmits: 10
    }
  );

  channel.binaryType = 'arraybuffer';

  channel.onopen = function() {
    dataChannels[addr] = channel;
  };

  channel.onmessage = function(event) {
    var data = event.data;
    var client = new WebSocketClient();

    client.on('connect', function(connection) {
      if (connection.connected)
        connection.sendUTF(data);
    });
    client.connect('ws://127.0.0.1:' + conductorPort);
  };

  pc.createOffer(function(offer) {
    pc.setLocalDescription(offer, function() {
      var client = mqtt.connect('mqtt://' + bridge + ':' + bridgePort);

      client.on('connect', function () {
        var args = {};

        args.key = nodeKey;
        args.offer = offer;
        args.cmd = 'prepare';
        args.addr = nodeAddress;
        args.bridge = bridgeAddr;
        client.publish(addr, encrypt(args, key));
        client.end();
      });
    });
  });

  peerConnections[addr] = {};
  peerConnections[addr].pc = pc;
  peerConnections[addr].ready = false;
}

function prepareChannel(addr, key, offer, bridge)
{
  if (addr in peerConnections)
    disconnect(addr);

  var pc = new RTCPeerConnection(
    {
      iceServers: [{url:'stun:23.31.150.121'}]
    },
    {
      'optional': []
    }
  );

  pc.setRemoteDescription(new RTCSessionDescription(offer), function() {
    pc.createAnswer(function(answer) {
      pc.setLocalDescription(new RTCSessionDescription(answer), function() {
        var client = mqtt.connect('mqtt://' + bridge + ':' + bridgePort);

        client.on('connect', function () {
          var args = {};

          args.cmd = 'create';
          args.answer = answer;
          args.addr = nodeAddress;
          client.publish(addr, encrypt(args, key));
          client.end();
        });
      });
    });
  });

  pc.ondatachannel = function(event) {
    var channel = event.channel;

    channel.onopen = function() {
      dataChannels[addr] = channel;
    };

    channel.onmessage = function(event) {
      var data = event.data;
      var client = new WebSocketClient();

      client.on('connect', function(connection) {
        if (connection.connected)
          connection.sendUTF(data);
      });
      client.connect('ws://127.0.0.1:' + conductorPort);
    };
  }

  pc.onicecandidate = function(event)
  {
    var candidate = event.candidate;

    if (!candidate)
      return;

    var client = mqtt.connect('mqtt://' + bridge + ':' + bridgePort);

    client.on('connect', function () {
      var args = {};

      args.cmd = 'candidate';
      args.cand = candidate;
      args.addr = nodeAddress;
      client.publish(addr, encrypt(args, key));
      client.end();
    });
  }

  peerConnections[addr] = {};
  peerConnections[addr].pc = pc;
  peerConnections[addr].ready = true;
}

function disconnect(addr)
{
  if (addr in dataChannels) {
    dataChannels[addr].close();
    delete dataChannels[addr];
  }

  if (addr in peerConnections) {
    if (peerConnections[addr].ready)
      peerConnections[addr].pc.close();
    delete peerConnections[addr];
  }
}

function writeChannel(addr, buf)
{
  if (addr in dataChannels && buf)
    dataChannels[addr].send(buf);
}

function createChannel(addr, answer)
{
  if (addr in peerConnections) {
    peerConnections[addr].pc.setRemoteDescription(new RTCSessionDescription(answer));
    peerConnections[addr].ready = true;
  }
}

function addCandidate(addr, cand)
{
  if (addr in peerConnections && cand) {
    var candidate = new RTCIceCandidate(cand);

    peerConnections[addr].pc.addIceCandidate(candidate);
  }
}

listener.on('connect', function() {
    listener.subscribe(nodeAddress);
});

listener.on('message', function(topic, message) {
  var data = decrypt(message.toString());
  var args = JSON.parse(data);

  if ('prepare' == args.cmd) {
    prepareChannel(args.addr, args.key, args.offer, args.bridge);
  } else if ('create' == args.cmd) {
    createChannel(args.addr, args.answer);
  } else if ('candidate' == args.cmd) {
    addCandidate(args.addr, args.cand);
  }
});

server.on('connection', function(ws) {
  ws.on('message', function(data) {
    var args = JSON.parse(data);

    if ('open' == args.cmd) {
      connect(args.addr, args.key, args.bridge);
    } else if ('write' == args.cmd) {
      writeChannel(args.addr, args.buf);
    } else if ('close' == args.cmd) {
      disconnect(args.addr);
    } else if ('exist' == args.cmd) {
      var addr = args.addr;

      if (addr in peerConnections && peerConnections[addr].ready)
        ws.send('exist');
      else
        ws.send('');
    }
  });
});
