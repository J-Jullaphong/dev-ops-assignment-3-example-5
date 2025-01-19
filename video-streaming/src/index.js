const express = require("express");
const fs = require("fs");
const amqp = require('amqplib');
const mongodb = require('mongodb')

if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

if (!process.env.DBHOST) {
    throw new Error("Please specify the databse host using environment variable DBHOST.");
}

if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME");
}

const PORT = process.env.PORT;
const RABBIT = process.env.RABBIT;
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;

//
// Application entry point.
//
async function main() {
	
    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    const messagingConnection = await amqp.connect(RABBIT); // Connects to the RabbitMQ server.
    
    console.log("Connected to RabbitMQ.");

    const messageChannel = await messagingConnection.createChannel(); // Creates a RabbitMQ messaging channel.

    const client = await mongodb.MongoClient.connect(DBHOST);
    const db = client.db(DBNAME);
    const videosCollection = db.collection("videos");

	await messageChannel.assertExchange("viewed", "fanout"); // Asserts that we have a "viewed" exchange.

    //
    // Broadcasts the "viewed" message to other microservices.
    //
	function broadcastViewedMessage(messageChannel, videoId, videoPath) {
	    console.log(`Publishing message on "viewed" exchange.`);
	        
	    const msg = { videoId: videoId, videoPath: videoPath };
	    const jsonMsg = JSON.stringify(msg);
	    messageChannel.publish("viewed", "", Buffer.from(jsonMsg)); // Publishes message to the "viewed" exchange.
	}

    const app = express();

    app.get("/video", async (req, res) => { // Route for streaming video.
        
        const videoId = parseInt(req.query.id, 10);
        const videoRecord = await videosCollection.findOne({ _id: videoId });
        if (!videoRecord) {
            // The video was not found.
            res.sendStatus(404);
            return;
        }

        console.log(`Translated id ${videoId} to path ${videoRecord.videoPath}.`);
        
        const videoPath = `./videos/${videoRecord.videoPath}`;
        const stats = await fs.promises.stat(videoPath);

        res.writeHead(200, {
            "Content-Length": stats.size,
            "Content-Type": "video/mp4",
        });
    
        fs.createReadStream(videoPath).pipe(res);

        broadcastViewedMessage(messageChannel, videoId, videoPath); // Sends the "viewed" message to indicate this video has been watched.
    });

    app.listen(PORT, () => {
        console.log("Microservice online.");
    });
}

main()
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });
