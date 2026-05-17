import amqplib from 'amqplib';
import DLQModel from './model.schema.js';
import nodemailer from 'nodemailer';
import dotenv from 'dotenv';
dotenv.config();

const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com",
    port: 587,
    secure: false,
    auth: {
        user: process.env.MAIL_USER,
        pass: process.env.MAIL_PASS
    }
})


async function mailSender(email, subject, message, html) {

    try{
        await transporter.sendMail({
            from: process.env.MAIL_USER,
            to: email,
            subject: subject,
            text: message,
            html: html || undefined
        })
    }catch(error){
        console.error("Error sending mail:", error);
    }
}


export async function consumer() {
    try {
        const connection = await amqplib.connect(process.env.AMQP_URL);
        const channel = await connection.createChannel();

        const dlqExchange = "dlq_exchange";
        const dlqQueue = "dlq_queue";
        const routingKey = "bind_key";

        const mailExchange = "mail_exchange";
        const mailQueue = "mail_queue";

        // DLQ configuration
        await channel.assertExchange(dlqExchange, "direct", { durable: true });
        await channel.assertQueue(dlqQueue, { durable: true });
        await channel.bindQueue(dlqQueue, dlqExchange, routingKey);

        // Mail queue configuration with DLQ settings
        // Note: DLQ arguments must be set during queue assertion
        await channel.assertExchange(mailExchange, "direct", { durable: true });
        await channel.assertQueue(mailQueue, {
            durable: true,
            arguments: {
                "x-dead-letter-exchange": dlqExchange,
                "x-dead-letter-routing-key": routingKey
            }
        });
        await channel.bindQueue(mailQueue, mailExchange, routingKey);

        await channel.prefetch(1);

        console.log("Waiting for messages in mail_queue and dlq_queue...");

        // Consume from mail queue
        await channel.consume(mailQueue, async (msg) => {
            if (msg) {
                try {
                    const data = JSON.parse(msg.content.toString());
                    console.log("Message received from mail_queue: ", data);

                    // Logic for sending mail would go here
                    console.log("getting the data in consumer in mail_service : ", data);
                    await mailSender(data.email, data.subject, data.message, data.html);
                    channel.ack(msg);
                    console.log("Message acknowledged from mail_queue");
                } catch (err) {
                    console.error("Error processing mail_queue message:", err);
                    // nack and send to DLQ (requeue: false)
                    channel.nack(msg, false, false);
                }
            }
        });

        // Consume from DLQ queue
        await channel.consume(dlqQueue, async (msg) => {
            if (msg) {
                try {
                    const data = JSON.parse(msg.content.toString());
                    console.log("Message received from dlq_queue: ", data);

                    // Try to extract reason from RabbitMQ headers
                    const xDeath = msg.properties.headers?.['x-death'];
                    const reason = xDeath && xDeath.length > 0
                        ? `Failed in queue: ${xDeath[0].queue} | Reason: ${xDeath[0].reason}`
                        : "Unknown failure reason";

                    // store the failed msg to DB
                    const failedMsg = new DLQModel({
                        to: data.email || "Unknown",
                        subject: data.subject || "No Subject",
                        text: data.message || "",
                        html: data.html || "",
                        reason: reason
                    });
                    await failedMsg.save();

                    console.log("Message saved to DLQ Database");
                    channel.ack(msg);
                } catch (err) {
                    console.error("Error processing dlq_queue message:", err);
                    // nack and don't requeue to avoid infinite loops in DLQ
                    channel.nack(msg, false, false);
                }
            }
        });

    } catch (err) {
        console.error("Consumer error:", err);
    }
}



