import { MyDatabase } from "./db/database";
import { Address } from "@ton/ton";
import {
    highloadAddress,
    serviceChatID,
} from "./config";
import { handleTransactions } from "./services/indexer/indexer";
import { configDotenv } from "dotenv";
import { Bot } from "grammy";
import { sleep } from "./helpers";
import {PoolConfig} from "pg";
import { Api, HttpClient } from "tonapi-sdk-js";

export async function retry<T>(
    fn: () => Promise<T>,
    attempts: number,
    timeout: number,
    title: string
): Promise<T> {
    let lastError = null;

    for (let i = 0; i < attempts; i++) {
        try {
            return await fn();
        } catch (error) {
            lastError = error;
            console.log(`[${title}] Attempt ${i + 1} failed. Retrying in ${timeout}ms...`);
            await sleep(timeout);
        }
    }

    throw lastError;
}

async function main(bot: Bot) {
    configDotenv();
    const pgConfig: PoolConfig = {
        max: parseInt(process.env.DB_MAX_CONNECTIONS) || 5,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
        host: process.env.DB_HOST,
        port: Number(process.env.DB_PORT) || 5432,
    };
    const db = new MyDatabase(pgConfig);
    await db.init();

    const httpClient = new HttpClient({
        baseUrl: 'https://tonapi.io',
        baseApiParams: {
            headers: {
                Authorization: process.env.TONAPI_KEY,
                'Content-type': 'application/json'
            }
        }
    });
    const client = new Api(httpClient);

    console.log(`Indexer is syncing...`);
    await handleTransactions(db, client, bot, Address.parse(highloadAddress), true);
    console.log(`Indexer is synced. Waiting 5 sec before starting`);

    await sleep(5000);
    const tick = async () => {
        console.log('Starting handleTransactions...')
        try {
            await handleTransactions(db, client, bot, Address.parse(highloadAddress));
        } catch (e) {
            console.log(e);
            await retry(async () => {     
                if (JSON.stringify(e).length == 2) {
                    await bot.api.sendMessage(serviceChatID, `[Indexer]: ${e}`);
                    return;
                }
                await bot.api.sendMessage(serviceChatID, `[Indexer]: ${JSON.stringify(e).slice(0, 300)}`);
            }, 3, 5000, 'Error Logging');
        }

        setTimeout(tick, 2000);
    }

    tick();
}

(() => {
    configDotenv();
    const bot = new Bot(process.env.TELEGRAM_BOT_TOKEN);
    main(bot)
        .catch(async e => {
            console.log(e);
            await retry(async () => {
                if (JSON.stringify(e).length == 2) {
                    await bot.api.sendMessage(serviceChatID, `Fatal error: ${e}`);
                    return;
                }
                await bot.api.sendMessage(serviceChatID, `Fatal error: ${JSON.stringify(e).slice(0, 300)} `)
            }, 2, 60000, "Fatal error");
        })
        .finally(() => console.log("Exiting..."));
})()
