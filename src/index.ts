import { MyDatabase } from './db/database';
import { isAxiosError } from 'axios';
import { createAssetConfig, createAssetData, getFriendlyAddress, getRequest } from './helpers';
import { MASTER_ADDRESS, OPCODES } from './config';
import { Dictionary, TonClient, Transaction, TupleReader } from '@ton/ton';
import { Bot, GrammyError, HttpError } from 'grammy';
import { configDotenv } from 'dotenv';
import { Address } from '@ton/core';
import { Log, TxType } from './db/types';
import { PoolConfig } from 'pg';

const UINT64_MAX = 18446744073709551615n;

configDotenv();
export const SERVICE_CHAT_ID = parseInt(process.env.SERVICE_CHAT_ID);

export type GetResult = {
    gas_used: number;
    stack: TupleReader;
    exit_code: number;
};

async function handleTransactions(db: MyDatabase, tonClient: TonClient, bot: Bot) {
    let before_lt = 0n;
    let attempts = 0;
    const startLT = await db.getFirstTransactionLt();
    while (true) {
        let transactions: Transaction[] = [];
        try {
            transactions = await tonClient.getTransactions(MASTER_ADDRESS, {
                limit: 100,
                to_lt: before_lt === 0n ? undefined : (before_lt - 1n).toString(),
            });
            attempts = 0;
        } catch (e) {
            attempts++;
            if (attempts > 3) {
                await bot.api.sendMessage(SERVICE_CHAT_ID, `🚨🚨🚨 Unknown problem with TonAPI 🚨🚨🚨`);
                console.log(e);
                await sleep(10000);
                attempts = 0;
            }
            await sleep(1500);
        }
        if (!transactions) {
            await sleep(1500);
            continue;
        }

        if (transactions.length === 0) {
            await sleep(1500);
            continue;
        }

        const first = await db.isTxExists(transactions[0].hash().toString('hex'));
        if (first) {
            if (before_lt !== 0n) {
                before_lt = 0n;
            }
            await sleep(1500);
            continue;
        }

        for (const transaction of transactions) {
            const hash = transaction.hash().toString('hex');
            const lt = transaction.lt;
            const result = await db.isTxExists(hash);
            if (result) continue;
            if (lt < startLT) break;
            if (transaction.inMessage.info.type !== 'internal') {
                console.log(`Skipping transaction ${hash}. Not internal.`);
                await db.addTransaction(hash, transaction.now, lt, 0n, true);
                continue;
            }
            if (!(transaction.description.type === 'generic' && transaction.description.computePhase.type === 'vm')) {
                console.log(`Skipping transaction ${hash}. Not generic or not vm.`);
                await db.addTransaction(hash, transaction.now, lt, 0n, true);
                continue;
            }
            if (transaction.outMessagesCount === 0) {
                console.log(`Skipping transaction ${hash}. No out messages.`);
                await db.addTransaction(hash, transaction.now, lt, 0n, true);
                continue;
            }

            console.log('Indexing transaction:', hash);
            const inMsgBody = transaction.inMessage.body.beginParse();
            const opcode = inMsgBody.loadUint(32);
            if (
                opcode === OPCODES.JETTON_TRANSFER_NOTIFICATION ||
                opcode === OPCODES.SUPPLY ||
                opcode === OPCODES.WITHDRAW ||
                opcode === OPCODES.LIQUIDATE
            ) {
                const outMsg = transaction.outMessages.get(0);
                if (outMsg.info.type !== 'internal') {
                    console.log(`Skipping transaction ${hash}. Out message is not internal. Opcode: ${opcode}`);
                    await db.addTransaction(hash, transaction.now, lt, 0n, true);
                    continue;
                }
                const outBody = outMsg.body.beginParse();
                outBody.loadCoins(); // expected version
                outBody.loadMaybeRef(); // upgrade info
                outBody.loadInt(2); // upgrade exec
                const outOpcode = outBody.loadUint(32);
                if (
                    outOpcode === OPCODES.SUPPLY_USER ||
                    outOpcode === OPCODES.WITHDRAW_USER ||
                    outOpcode === OPCODES.LIQUIDATE_USER
                ) {
                    let senderAddress: Address;
                    if (opcode === OPCODES.JETTON_TRANSFER_NOTIFICATION) {
                        inMsgBody.skip(64); // query_id
                        inMsgBody.loadCoins();
                        senderAddress = inMsgBody.loadAddress();
                    } else {
                        senderAddress = transaction.inMessage.info.src;
                    }

                    const contractAddress = outMsg.info.dest;
                    await db.addTransaction(
                        hash,
                        transaction.now,
                        lt,
                        outMsg.info.createdLt,
                        false,
                        senderAddress,
                        contractAddress,
                    );
                    continue;
                }
            } else if (opcode === OPCODES.WITHDRAW_COLLATERIZED || opcode === OPCODES.LIQUIDATE_SATISFIED) {
                const outMsg = transaction.outMessages.get(0);
                if (outMsg.info.type !== 'internal') {
                    console.log(`Skipping transaction ${hash}. Out message is not internal. Opcode: ${opcode}`);
                    await db.addTransaction(hash, transaction.now, lt, 0n, true);
                    continue;
                }
                const outBody = outMsg.body.beginParse();
                outBody.loadCoins(); // expected version
                outBody.loadMaybeRef(); // upgrade info
                outBody.loadInt(2); // upgrade exec
                const outOpcode = outBody.loadUint(32);
                await db.addOperation({
                    id: 0,
                    hash: hash,
                    opcode: outOpcode,
                    inMsgLt: transaction.inMessage.info.createdLt,
                    inMsgBody: transaction.inMessage.body.toBoc({ crc32: false }).toString('hex'),
                    createdAt: new Date(),
                });
            }

            await db.addTransaction(hash, transaction.now, lt, 0n, true);
            before_lt = transaction.lt;
        }

        await sleep(1000);
    }
}

async function getUserPrincipal(
    contractAddress: Address,
    tonClient: TonClient,
    assetID: bigint,
    bot: Bot,
): Promise<bigint> {
    let attempts = 0;
    let userDataSuccess = false;
    let userDataResult: GetResult;
    while (true) {
        try {
            userDataResult = await tonClient.runMethodWithError(contractAddress, 'getAllUserScData');
            if (userDataResult.exit_code === 0) {
                userDataSuccess = true;
                break;
            }
            attempts++;
            if (attempts > 10) {
                break;
            }
            await sleep(500);
        } catch (e) {
            console.log(e);
            attempts++;
            if (attempts > 10) {
                throw e;
            }
            if (!isAxiosError(e)) {
                throw e;
            }
            await sleep(500);
        }
    }
    if (!userDataSuccess) {
        await bot.api.sendMessage(SERVICE_CHAT_ID, `Problem with user contract ${getFriendlyAddress(contractAddress)}`);
        await sleep(300);
        throw new Error(`Problem with user contract ${getFriendlyAddress(contractAddress)}`);
    }
    if (userDataResult.exit_code !== 0) {
        console.log(userDataResult);
        await bot.api.sendMessage(SERVICE_CHAT_ID, `User contract failed ${getFriendlyAddress(contractAddress)}`);
        await sleep(300);
        throw new Error(`User contract failed ${getFriendlyAddress(contractAddress)}`);
    }
    const codeVersion = userDataResult.stack.readNumber();
    userDataResult.stack.readCell(); // master
    const userAddress = userDataResult.stack.readCell().beginParse().loadAddress();
    const principalsDict = userDataResult.stack
        .readCellOpt()
        ?.beginParse()
        .loadDictDirect(Dictionary.Keys.BigUint(256), Dictionary.Values.BigInt(64));

    const principal = principalsDict.get(assetID);
    if (principal === undefined) {
        console.log(assetID);
        console.log(principalsDict);
        await bot.api.sendMessage(SERVICE_CHAT_ID, `Principal not found for ${getFriendlyAddress(contractAddress)}`);
        await sleep(300);
        throw new Error('Principal not found');
    }

    return principal;
}

async function processTransactions(db: MyDatabase, tonClient: TonClient, bot: Bot) {
    const tasks = await db.getUnprocessedTransactions();
    for (const task of tasks) {
        const transactions = await tonClient.getTransactions(task.contractAddress, {
            limit: 100,
            to_lt: (task.nextMsgLt - 1n).toString(),
        });
        // const result = await tonApi.get(getRequest(task.contractAddress, 0, 100, task.nextMsgLt - 1));
        // const transactions: Transaction[] = result.data.transactions;
        for (const transaction of transactions) {
            if (transaction.inMessage.info.type !== 'internal') continue;
            const inMsgCreatedLt = transaction.inMessage.info.createdLt;
            if (inMsgCreatedLt < task.nextMsgLt) break;
            if (inMsgCreatedLt > task.nextMsgLt) continue;
            let check = true;
            if (transaction.description.type !== 'generic') {
                await db.processTransaction(transaction.hash().toString('hex'));
                return;
            }
            if (transaction.description.computePhase.type !== 'vm') {
                await db.processTransaction(transaction.hash().toString('hex'));
                return;
            }
            if (inMsgCreatedLt === task.nextMsgLt) {
                if (!transaction.description.computePhase.success) {
                    await db.processTransaction(transaction.hash().toString('hex'));
                    return;
                }
                const outMsg = transaction.outMessages.get(0);
                if (outMsg.info.type !== 'internal') {
                    await db.processTransaction(transaction.hash().toString('hex'));
                    return;
                }
                const body = transaction.inMessage.body.beginParse();
                let outBody = outMsg.body.beginParse();
                const outOpcode = outBody.loadUint(32);
                body.loadCoins(); // expected version
                body.loadMaybeRef(); // upgrade info
                body.loadInt(2); // upgrade exec
                const opcode = body.loadUint(32);
                body.skip(64); // query_id

                console.log('Processing transaction:', task.hash);
                if (opcode === OPCODES.SUPPLY_USER) {
                    if (outOpcode !== OPCODES.SUPPLY_SUCCESS) {
                        await db.processTransaction(task.hash);
                        continue;
                    }

                    const assetID = body.loadUintBig(256);
                    const amount = body.loadUintBig(64);
                    const sRate = body.loadUintBig(64);
                    const bRate = body.loadUintBig(64);

                    outBody.skip(64); // query_id
                    const userAddress = outBody.loadAddress();
                    outBody.skip(256); // assetID
                    const amountSupplied = outBody.loadUintBig(64);

                    if (amount !== amountSupplied) {
                        await bot.api.sendMessage(
                            SERVICE_CHAT_ID,
                            `[Supply] Amounts are not equal. Expected: ${amount}, got: ${amountSupplied}`,
                        );
                        await sleep(300);
                    }

                    let assetPrincipal: bigint;
                    try {
                        assetPrincipal = await getUserPrincipal(task.contractAddress, tonClient, assetID, bot);
                    } catch (e) {
                        console.error(e);
                        break;
                    }

                    const log: Log = {
                        utime: transaction.now,
                        txType: TxType.SUPPLY,
                        senderAddress: task.senderAddress,
                        userAddress: userAddress,
                        outLt: 0n,
                        outBody: '',
                        attachedAssetAddress: assetID,
                        attachedAssetAmount: amount,
                        attachedAssetPrincipal: assetPrincipal,
                        attachedAssetTotalSupplyPrincipal: 0n,
                        attachedAssetTotalBorrowPrincipal: 0n,
                        attachedAssetSRate: sRate,
                        attachedAssetBRate: bRate,

                        redeemedAssetAddress: 0n,
                        redeemedAssetAmount: 0n,
                        redeemedAssetPrincipal: 0n,
                        redeemedAssetTotalSupplyPrincipal: 0n,
                        redeemedAssetTotalBorrowPrincipal: 0n,
                        redeemedAssetSRate: 0n,
                        redeemedAssetBRate: 0n,
                        processed: true,
                    };

                    await db.addAttachedAssetLog(log);
                    await db.processTransaction(task.hash);
                } else if (opcode === OPCODES.WITHDRAW_USER) {
                    if (outOpcode !== OPCODES.WITHDRAW_COLLATERIZED) {
                        await db.processTransaction(task.hash);
                        continue;
                    }

                    const assetID = body.loadUintBig(256);
                    const amount = body.loadUintBig(64);
                    const sRate = body.loadUintBig(64);
                    const bRate = body.loadUintBig(64);
                    const recipientAddress = body.loadAddress();
                    const assetConfig = body.loadDict(Dictionary.Keys.BigUint(256), createAssetConfig());
                    const assetData = body.loadDict(Dictionary.Keys.BigUint(256), createAssetData());
                    const totalSupply = assetData.get(assetID).totalSupply;
                    const totalBorrow = assetData.get(assetID).totalBorrow;

                    outBody.skip(64); // query_id
                    const userAddress = outBody.loadAddress();
                    outBody.skip(256); // assetID
                    const amountWithdrawn = outBody.loadUintBig(64);

                    if (amount !== UINT64_MAX && amount !== amountWithdrawn) {
                        await bot.api.sendMessage(
                            SERVICE_CHAT_ID,
                            `[Withdraw] Amounts are not equal. Expected: ${amount}, got: ${amountWithdrawn}`,
                        );
                        await sleep(300);
                    }

                    let assetPrincipal: bigint;
                    try {
                        assetPrincipal = await getUserPrincipal(task.contractAddress, tonClient, assetID, bot);
                    } catch (e) {
                        console.error(e);
                        break;
                    }

                    const log: Log = {
                        utime: transaction.now,
                        txType: TxType.WITHDRAW,
                        senderAddress: task.senderAddress,
                        userAddress: userAddress,
                        outLt: outMsg.info.createdLt,
                        outBody: outMsg.body.toBoc({ crc32: false }).toString('hex'),
                        attachedAssetAddress: 0n,
                        attachedAssetAmount: 0n,
                        attachedAssetPrincipal: 0n,
                        attachedAssetTotalSupplyPrincipal: 0n,
                        attachedAssetTotalBorrowPrincipal: 0n,
                        attachedAssetSRate: 0n,
                        attachedAssetBRate: 0n,

                        redeemedAssetAddress: assetID,
                        redeemedAssetAmount: amount,
                        redeemedAssetPrincipal: assetPrincipal,
                        redeemedAssetTotalSupplyPrincipal: totalSupply,
                        redeemedAssetTotalBorrowPrincipal: totalBorrow,
                        redeemedAssetSRate: sRate,
                        redeemedAssetBRate: bRate,
                        processed: false,
                    };

                    await db.addRedeemedAssetLog(log);
                    await db.processTransaction(task.hash);
                } else if (opcode === OPCODES.LIQUIDATE_USER) {
                    if (outOpcode !== OPCODES.LIQUIDATE_SATISFIED) {
                        await db.processTransaction(task.hash);
                        continue;
                    }
                    outBody.skip(64); // query_id
                    const userAddress = outBody.loadAddress();
                    const liquidatorAddress = outBody.loadAddress();
                    const attachedAssetID = outBody.loadUintBig(256);
                    outBody = outBody.loadRef().beginParse();
                    outBody.loadInt(64); // delta loan principal
                    const attachedAssetAmount = outBody.loadUintBig(64);
                    outBody.loadUint(64); // protocol gift
                    const redeemedAssetID = outBody.loadUintBig(256);
                    outBody.loadUintBig(64); // delta collateral principal
                    const redeemedAssetAmount = outBody.loadUintBig(64);

                    const assetConfig = body.loadDict(Dictionary.Keys.BigUint(256), createAssetConfig());
                    const assetData = body.loadDict(Dictionary.Keys.BigUint(256), createAssetData());
                    // const pricesPacked = body.loadRef();
                    // const redeemedAssetID = body.loadUintBig(256);
                    // const minCollateralAmount = body.loadUintBig(64);
                    // const liquidatorAddress = body.loadAddress();
                    // const attachedAssetID = body.loadUintBig(256);
                    // const attachedAssetAmount = body.loadUintBig(64);

                    let attachedAssetPrincipal: bigint;
                    let redeemedAssetPrincipal: bigint;
                    try {
                        attachedAssetPrincipal = await getUserPrincipal(
                            task.contractAddress,
                            tonClient,
                            attachedAssetID,
                            bot,
                        );
                        redeemedAssetPrincipal = await getUserPrincipal(
                            task.contractAddress,
                            tonClient,
                            redeemedAssetID,
                            bot,
                        );
                    } catch (e) {
                        console.error(e);
                        break;
                    }

                    const log: Log = {
                        utime: transaction.now,
                        txType: TxType.LIQUIDATE,
                        senderAddress: task.senderAddress,
                        userAddress: userAddress,
                        outLt: outMsg.info.createdLt,
                        outBody: outMsg.body.toBoc({ crc32: false }).toString('hex'),
                        attachedAssetAddress: attachedAssetID,
                        attachedAssetAmount: attachedAssetAmount,
                        attachedAssetPrincipal: attachedAssetPrincipal,
                        attachedAssetTotalSupplyPrincipal: assetData.get(attachedAssetID).totalSupply,
                        attachedAssetTotalBorrowPrincipal: assetData.get(attachedAssetID).totalBorrow,
                        attachedAssetSRate: assetData.get(attachedAssetID).sRate,
                        attachedAssetBRate: assetData.get(attachedAssetID).bRate,

                        redeemedAssetAddress: redeemedAssetID,
                        redeemedAssetAmount: redeemedAssetAmount,
                        redeemedAssetPrincipal: redeemedAssetPrincipal,
                        redeemedAssetTotalSupplyPrincipal: assetData.get(redeemedAssetID).totalSupply,
                        redeemedAssetTotalBorrowPrincipal: assetData.get(redeemedAssetID).totalBorrow,
                        redeemedAssetSRate: assetData.get(redeemedAssetID).sRate,
                        redeemedAssetBRate: assetData.get(redeemedAssetID).bRate,
                        processed: false,
                    };

                    await db.addLog(log);
                    await db.processTransaction(task.hash);
                }
            }
        }

        await sleep(1000);
    }
}

async function processLogs(db: MyDatabase, bot: Bot) {
    const unprocessedLogs = await db.getUnprocessedLogs();
    for (const log of unprocessedLogs) {
        const operation = await db.getOperation(log.outLt, log.outBody);
        if (!operation) {
            if (Math.floor(Date.now() / 1000) - log.utime > 180) {
                await bot.api.sendMessage(SERVICE_CHAT_ID, `[Log] Operation not found: ${log.outLt}`);
                await sleep(300);
            }
            continue;
        }

        if (operation.opcode === OPCODES.WITHDRAW_SUCCESS || operation.opcode === OPCODES.LIQUIDATE_SUCCESS) {
            await db.processLog(log.id!);
        } else if (operation.opcode === OPCODES.WITHDRAW_FAIL || operation.opcode === OPCODES.LIQUIDATE_FAIL) {
            await db.deleteLog(log.id!);
        }
    }
}

let handlingTransactions = false;
let processingTransactions = false;
let processingLogs = false;

let transactionID: NodeJS.Timeout;
let processingID: NodeJS.Timeout;
let processingLogsID: NodeJS.Timeout;

async function main(bot: Bot) {
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
    const tonClient = new TonClient({
        endpoint: process.env.RPC_ENDPOINT,
        apiKey: process.env.RPC_API_KEY,
    });

    const transactions = await tonClient.getTransactions(MASTER_ADDRESS, { limit: 1 });
    const txHash = transactions[0].hash().toString('hex');
    if (!(await db.isTxExists(txHash))) {
        await db.addTransaction(txHash, transactions[0].now, transactions[0].lt, 0n, true);
    }
    transactionID = setInterval(async () => {
        if (handlingTransactions) return;
        handlingTransactions = true;
        console.log('Starting handleTransactions...');
        handleTransactions(db, tonClient, bot)
            .catch(async (e) => {
                console.log(e);
                if (JSON.stringify(e).length == 2) {
                    await bot.api.sendMessage(SERVICE_CHAT_ID, `[Indexer]: ${e}`);
                    await sleep(300);
                    return;
                }
                await bot.api.sendMessage(SERVICE_CHAT_ID, `[Indexer]: ${JSON.stringify(e).slice(0, 600)}`);
                await sleep(300);
            })
            .finally(() => {
                mainDoings = false;
                handlingTransactions = false;
                console.log('Exiting from handleTransactions...');
                bot.api.sendMessage(SERVICE_CHAT_ID, `Exiting from handleTransactions`).catch((e) => {
                    console.log('bot error in handle finally: ');
                    console.log(e);
                });
            });
    }, 5000);

    processingID = setInterval(async () => {
        if (processingTransactions) return;
        processingTransactions = true;
        try {
            await processTransactions(db, tonClient, bot);
        } catch (e) {
            mainDoings = false;
            console.log(e);
            if (JSON.stringify(e).length == 2) {
                bot.api.sendMessage(SERVICE_CHAT_ID, `[Processing]: ${e}`);
                return;
            }
            bot.api.sendMessage(SERVICE_CHAT_ID, `[Processing]: ${JSON.stringify(e).slice(0, 600)}`);
        }
        processingTransactions = false;
    }, 5000);

    processingLogsID = setInterval(async () => {
        if (processingLogs) return;
        processingLogs = true;
        try {
            await processLogs(db, bot);
        } catch (e) {
            mainDoings = false;
            console.log(e);
            if (JSON.stringify(e).length == 2) {
                bot.api.sendMessage(SERVICE_CHAT_ID, `[ProcessingLogs]: ${e}`);
                return;
            }
            bot.api.sendMessage(SERVICE_CHAT_ID, `[ProcessingLogs]: ${JSON.stringify(e).slice(0, 600)}`);
        }
        processingLogs = false;
    }, 5000);
}

let mainDoings = false;
const bot = new Bot(process.env.BOT_TOKEN);
bot.catch((err) => {
    const ctx = err.ctx;
    console.error(`Error while handling update ${ctx.update.update_id}:`);
    const e = err.error;
    if (e instanceof GrammyError) {
        console.error("Error in request:", e.description);
    } else if (e instanceof HttpError) {
        console.error("Could not contact Telegram:", e);
    } else {
        console.error("Unknown error:", e);
    }
});

(() => {
    const mainID = setInterval(async () => {
        if (mainDoings) return;
        mainDoings = true;
        clearInterval(transactionID);
        clearInterval(processingID);
        clearInterval(processingLogsID);
        console.log('Starting main...');
        main(bot);
    }, 5000);
})();

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
