const FirehoseClient = require('../FirehoseClient')
const SymBotAuth = require('../SymBotAuth')
const PubSub = require('pubsub-js')

var FirehoseEventsService = {
  liveStatus: true
}

var FirehoseV2EventsService = {
  liveStatus: true
}

// Firehose V1 Service Initialisation
FirehoseEventsService.initService = (subscriberCallback) => {
  var psToken = PubSub.subscribe('FIREHOSE: MESSAGE_RECEIVED', subscriberCallback)
  FirehoseClient.registerBot(psToken)
  FirehoseEventsService.startFirehose()
}

// Firehose V1 Service Startup
FirehoseEventsService.startFirehose = () => {
  FirehoseClient.createFirehose().then((firehoseInstance) => {
    FirehoseEventsService.readFirehose(firehoseInstance.id)
  })
}

// Firehose V1 Service Shutdown
FirehoseEventsService.stopService = () => {
  console.log('FIREHOSE: The BOT ' + SymBotAuth.botUser.displayName + ' will be automatically stopped in the next 30 sec...')
  FirehoseEventsService.liveStatus = false
}

// Firehose V2 Service Initialisation
FirehoseV2EventsService.initService = (subscriberCallback) => {
  var psToken = PubSub.subscribe('FIREHOSE: MESSAGE_RECEIVED', subscriberCallback)
  FirehoseClient.registerBot(psToken)
  FirehoseV2EventsService.startFirehoseV2()
}

// Firehose V2 Service Startup
FirehoseV2EventsService.startFirehose = () => {
  FirehoseClient.createFirehoseV2().then((firehoseInstance) => {
    FirehoseV2EventsService.readFirehose(firehoseInstance.id)
  })
}

// Firehose V2 Service Shutdown
FirehoseV2EventsService.stopService = () => {
  console.log('FIREHOSEV2: The BOT ' + SymBotAuth.botUser.displayName + ' will be automatically stopped in the next 30 sec...')
  FirehoseEventsService.liveStatus = false
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
FirehoseV2EventsService.readFirehose = (firehoseId) => {
  if (FirehoseV2EventsService.liveStatus) {
    FirehoseClient.getEventsFromFirehoseV2(firehoseId).then((res) => {
      if (res.status === 'success') {
        FirehoseV2EventsService.readFirehose(firehoseId)
      } else if (res.status === 'timeout') {
        FirehoseV2EventsService.readFirehose(firehoseId)
      }
    })
  }
}

module.exports = { FirehoseEventsService, FirehoseV2EventsService }
