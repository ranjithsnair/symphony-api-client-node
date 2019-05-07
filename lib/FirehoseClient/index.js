const https = require('https')
const Q = require('kew')
const PubSub = require('pubsub-js')
const SymBotAuth = require('../SymBotAuth')
const SymConfigLoader = require('../SymConfigLoader')
const SymMessageParser = require('../SymMessageParser')
const request = require('../Request')

var FirehoseClient = {}

FirehoseClient.registerBot = (token) => {
  FirehoseClient.psToken = token
}

// Create Firehose v1.0
FirehoseClient.createFirehose = () => {
  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/agent/v4/firehose/create',
    'method': 'POST',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  return request(options, 'FirehoseClient/createFirehose')
}

// Read Firehose v1.0
FirehoseClient.getEventsFromFirehose = (firehoseId) => {
  var defer = Q.defer()

  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/agent/v4/firehose/' + firehoseId + '/read',
    'method': 'GET',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  var req = https.request(options, function (res) {
    var str = ''
    res.on('data', function (chunk) {
      str += chunk
    })
    res.on('end', function () {
      if (SymBotAuth.debug) {
        console.log('[DEBUG]', 'FirehoseClient/getEventsFromFirehose/res.statusCode', res.statusCode)
        console.log('[DEBUG]', 'FirehoseClient/getEventsFromFirehouse/str', str)
      }
      if (res.statusCode === 200) {
        PubSub.publish('FIREHOSE: MESSAGE_RECEIVED', SymMessageParser.parse(str))
        defer.resolve({ 'status': 'success' })
      } else if (res.statusCode === 204) {
        defer.resolve({ 'status': 'timeout' })
      } else {
        defer.reject({ 'status': 'error' })
      }
    })
  })
  req.on('error', function (e) {
    defer.reject({ 'status': 'error' })
  })

  req.end()

  return defer.promise
}

// Create Firehose v2.0
FirehoseClient.createFirehoseV2 = () => {
  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/agent/v5/firehose/create',
    'method': 'POST',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  return request(options, 'FirehoseClient/createFirehoseV2')
}

// Read Firehose v2.0
FirehoseClient.getEventsFromFirehoseV2 = (firehoseId, ackId, maxMsgs, timeout) => {
  var defer = Q.defer()

  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/agent/v5/firehose/' + firehoseId + '/read',
    'method': 'GET',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  var body = {
    'ackId': ackId,
    'maxMsgs': maxMsgs,
    'timeout': timeout
  }

  var req = https.request(options, function (res) {
    var str = ''
    res.on('data', function (chunk) {
      str += chunk
    })
    res.on('end', function () {
      if (SymBotAuth.debug) {
        console.log('[DEBUG]', 'FirehoseClient/getEventsFromFirehoseV2/res.statusCode', res.statusCode)
        console.log('[DEBUG]', 'FirehoseClient/getEventsFromFirehouseV2/str', str)
      }
      if (res.statusCode === 200) {
        PubSub.publish('FIREHOSEV2: MESSAGE_RECEIVED', SymMessageParser.parse(str))
        defer.resolve({ 'status': 'success' })
      } else if (res.statusCode === 204) {
        defer.resolve({ 'status': 'timeout' })
      } else {
        defer.reject({ 'status': 'error' })
      }
    })
  })
  req.on('error', function (e) {
    defer.reject({ 'status': 'error' })
  })

  req.write(body)
  req.end()

  return defer.promise
}

// Delete Firehose v2.0
FirehoseClient.deleteFirehoseV2 = (firehoseId) => {
  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/agent/v5/firehose/' + firehoseId + '/delete',
    'method': 'DELETE',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  return request(options, 'FirehoseClient/deleteFirehoseV2')
}

// List Firehose v2.0
FirehoseClient.listFirehoseV2 = () => {
  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/agent/v5/firehose/list',
    'method': 'GET',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  return request(options, 'FirehoseClient/listFirehoseV2')
}

// List Firehose v2.0
FirehoseClient.healthCheckFirehoseV2 = () => {
  var options = {
    'hostname': SymConfigLoader.SymConfig.agentHost,
    'port': SymConfigLoader.SymConfig.agentPort,
    'path': '/firehose2/v1/health',
    'method': 'GET',
    'headers': {
      'sessionToken': SymBotAuth.sessionAuthToken,
      'keyManagerToken': SymBotAuth.kmAuthToken
    },
    'agent': SymConfigLoader.SymConfig.agentProxy
  }

  return request(options, 'FirehoseClient/healthCheckFirehoseV2')
}

module.exports = FirehoseClient
