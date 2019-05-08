const FirehoseClient = require('../FirehoseClient')
const PubSub = require('pubsub-js')
const SymBotAuth = require('../SymBotAuth')
const SymConfigLoader = require('../SymConfigLoader')

var FirehoseEventsService = {
  liveStatus: true
}

// Firehose Service Initialisation
FirehoseEventsService.initService = (subscriberCallback) => {
  var psToken

  if (SymConfigLoader.SymConfig.firehoseType === '1') {
    psToken = PubSub.subscribe('FIREHOSEV1: MESSAGE_RECEIVED', subscriberCallback)
    FirehoseClient.registerBot(psToken)
    FirehoseEventsService.startFirehose()
  } else {
    psToken = PubSub.subscribe('FIREHOSEV2: MESSAGE_RECEIVED', subscriberCallback)
    FirehoseClient.registerBot(psToken)
    FirehoseEventsService.startFirehoseV2()
  }
}

// Firehose Service Shutdown
FirehoseEventsService.stopService = () => {
  if (SymConfigLoader.SymConfig.firehoseType === '1') {
    console.log('FIREHOSEV1: The BOT ' + SymBotAuth.botUser.displayName + ' will be automatically stopped in the next 30 sec...')
    FirehoseEventsService.liveStatus = false
  } else {
    console.log('FIREHOSEV2: The BOT ' + SymBotAuth.botUser.displayName + ' will be automatically stopped in the next 30 sec...')
    FirehoseEventsService.liveStatus = false
  }
}

// Firehose V1 Service Startup
FirehoseEventsService.startFirehose = () => {
  FirehoseClient.createFirehose().then((firehoseInstance) => {
    FirehoseEventsService.readFirehose(firehoseInstance.id)
  })
}

// Firehose V2 Service Startup
FirehoseEventsService.startFirehoseV2 = () => {
  FirehoseClient.createFirehoseV2().then((firehoseInstance) => {
    FirehoseEventsService.readFirehoseV2(firehoseInstance.id)
  })
}

// Firehose V1 Read Feed
FirehoseEventsService.readFirehose = (firehoseId) => {
  if (FirehoseEventsService.liveStatus) {
    FirehoseClient.getEventsFromFirehose(firehoseId).then((res) => {
      if (res.status === 'success') {
        FirehoseEventsService.readFirehose(firehoseId)
      } else if (res.status === 'timeout') {
        FirehoseEventsService.readFirehose(firehoseId)
      }
    })
  }
}

// Firehose V2 Read Feed
FirehoseEventsService.readFirehoseV2 = (firehoseId) => {
  if (FirehoseEventsService.liveStatus) {
    FirehoseClient.getEventsFromFirehoseV2(firehoseId).then((res) => {
      if (res.status === 'success') {
        FirehoseEventsService.readFirehoseV2(firehoseId)
      } else if (res.status === 'timeout') {
        FirehoseEventsService.readFirehoseV2(firehoseId)
      }
    })
  }
}

module.exports = FirehoseEventsService
