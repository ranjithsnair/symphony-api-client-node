const PubSub = require('pubsub-js')
const SymBotAuth = require('../SymBotAuth')
const SymConfigLoader = require('../SymConfigLoader')
const SymMessageParser = require('../SymMessageParser')
const request = require('../Request')
const { agentRequest } = require('../Request/clients')

const FirehoseClient = {}

FirehoseClient.registerBot = token => {
  FirehoseClient.psToken = token
}

// Create Firehose v1.0 using https://rest-api.symphony.com/v1.53/reference#create-firehose-v4
FirehoseClient.createFirehose = () =>
  agentRequest('post', '/agent/v4/firehose/create', 'FirehoseClient/createFirehose')

// Read Firehose v1.0 using https://rest-api.symphony.com/v1.53/reference#read-firehose-v4
FirehoseClient.getEventsFromFirehose = firehoseId => {
  const options = {
    hostname: SymConfigLoader.SymConfig.agentHost,
    port: SymConfigLoader.SymConfig.agentPort,
    path: `/agent/v4/firehose/${firehoseId}/read`,
    method: 'GET',
    headers: {
      sessionToken: SymBotAuth.sessionAuthToken,
      keyManagerToken: SymBotAuth.kmAuthToken
    },
    agent: SymConfigLoader.SymConfig.agentProxy
  }

  return request(options, 'FirehoseClient/getEventsFromFirehouse', null, true).then(response => {
    if (SymBotAuth.debug) {
      console.log(
        '[DEBUG]',
        'FirehoseClient/getEventsFromFirehose/res.statusCode',
        response.statusCode
      )
    }

    if (response.statusCode === 200) {
      PubSub.publish('FIREHOSE: MESSAGE_RECEIVED', SymMessageParser.parse(response.body))
      return { status: 'success' }
    } else if (response.statusCode === 204) {
      return { status: 'timeout' }
    } else {
      throw { status: 'error' }
    }
  })
}

// Create Firehose v2.0
FirehoseClient.createFirehoseV2 = () =>
  agentRequest('post', '/agent/v5/firehose/create', 'FirehoseClient/createFirehoseV2')

// Read Firehose v2.0
FirehoseClient.getEventsFromFirehoseV2 = (firehoseId, ackId, maxMsgs = 1000, timeout = 10000) => {
  const options = {
    hostname: SymConfigLoader.SymConfig.agentHost,
    port: SymConfigLoader.SymConfig.agentPort,
    path: `/agent/v5/firehose/${firehoseId}/read`,
    method: 'GET',
    headers: {
      sessionToken: SymBotAuth.sessionAuthToken,
      keyManagerToken: SymBotAuth.kmAuthToken
    },
    agent: SymConfigLoader.SymConfig.agentProxy
  }

  const body = {
    'ackId': ackId,
    'maxMsgs': maxMsgs,
    'timeout': timeout
  }

  return request(options, 'FirehoseClient/getEventsFromFirehouseV2', body, true).then(response => {
    if (SymBotAuth.debug) {
      console.log(
        '[DEBUG]',
        'FirehoseClient/getEventsFromFirehoseV2/res.statusCode',
        response.statusCode
      )
    }

    if (response.statusCode === 200) {
      PubSub.publish('FIREHOSEV2: MESSAGE_RECEIVED', SymMessageParser.parse(response.body))
      return { status: 'success' }
    } else if (response.statusCode === 204) {
      return { status: 'timeout' }
    } else {
      throw { status: 'error' }
    }
  })
}

// Delete Firehose v2.0
FirehoseClient.deleteFirehoseV2 = (firehoseId) =>
  agentRequest('delete', `/agent/v5/firehose/${firehoseId}/delete`, 'FirehoseClient/deleteFirehoseV2')

// List Firehose v2.0
FirehoseClient.listFirehoseV2 = (firehoseId) =>
  agentRequest('delete', `/agent/v5/firehose/${firehoseId}/list`, 'FirehoseClient/listFirehoseV2')

// Health Check Firehose v2.0
FirehoseClient.healthCheckFirehoseV2 = () =>
  agentRequest('get', `/firehose/v1/health`, 'FirehoseClient/healthCheckFirehoseV2')

module.exports = FirehoseClient
