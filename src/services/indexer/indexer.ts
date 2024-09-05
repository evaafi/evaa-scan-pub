import {MyDatabase} from "../../db/database";
import {AxiosInstance, AxiosResponse, isAxiosError} from "axios";
import {AssetID, decimals, evaaMaster, serviceChatID} from "../../config";
import {getAddressFriendly, getAssetName, getFriendlyAmount, getRequest} from "./helpers";
import {sleep} from "../../helpers";
import {Address, Transaction} from "@ton/core";
import {GetResult, UserPrincipals} from "./types";
import {Cell, Dictionary, TonClient} from "@ton/ton";
import {Bot} from "grammy";
import * as fs from "fs";
import { Api, Transactions } from "tonapi-sdk-js";
import { Log, TxType } from "../../db/types";
import { retry } from "../..";

let lastRpcCall = 0;

const errorCodes = {
    0x30F1: "Master liquidating too much",
    0x31F2: "Not liquidatable",
    0x31F3: "Min collateral not satisfied",
    0x31F4: "User not enough collateral",
    0x31F5: "User liquidating too much",
    0x31F6: "Master not enough liquidity",
    0x31F0: "User withdraw in process"
}

export async function handleTransactions(db: MyDatabase, tonApi: Api<unknown>, bot: Bot, walletAddress: Address, sync = false) {
    let before_lt = 0;
    let attempts = 0;
    while (true) {
        let result: Transactions;
        try {
            result = await tonApi.blockchain.getBlockchainAccountTransactions(evaaMaster.toString(), {
                before_lt: before_lt === 0 ? undefined : before_lt,
                limit: 1000
            });
            attempts = 0;
        } catch (e) {
            attempts++;
            if (attempts > 3) {
                await bot.api.sendMessage(serviceChatID, `🚨🚨🚨 Unknown problem with TonAPI 🚨🚨🚨`);
                console.log(e);
                await sleep(10000);
                attempts = 0;
            }
            await sleep(1000);
            continue;
        }
        const transactions = result.transactions;
        if (transactions.length === 0) break;
        const firstTxHash = BigInt('0x' + transactions[0].hash);
        const first = await db.isTxExists(firstTxHash);
        if (first) {
            if (sync) break;
            if (before_lt !== 0) {
                console.log(`Resetting before_lt to 0. Before lt was: ${before_lt}`);
                before_lt = 0;
            }
            await sleep(1000);
            continue;
        }

        transactions.sort((a, b) => b.lt - a.lt);
        for (const transaction of transactions) {
            const hash = BigInt('0x' + transaction.hash);
            const utime = transaction.utime;
            if (utime < 1716051631) {
                return;
            };
            const addResult = await db.addTransaction(hash, utime);
            if (addResult) {
                console.log(`Transaction ${hash} already added`);
                break;
            };
            console.log(`Transaction ${hash.toString(16)} added`);
            before_lt = transaction.lt;

            let opStr = transaction.in_msg ? transaction.in_msg.op_code : undefined;
            if (opStr === undefined) continue;
            const op = parseInt(opStr);
            if (op === 0x11a || op === 0x211 || op === 0x311) {
                if (transaction.compute_phase === undefined || transaction.action_phase === undefined) continue;
                if (!(transaction.compute_phase.success === true)) continue;
                if (!(transaction.action_phase.success === true)) continue;
                transaction.out_msgs.sort((a, b) => a.created_lt - b.created_lt);
                const outMsgs = transaction.out_msgs;
                const reportMsgBody = Cell.fromBoc(Buffer.from(outMsgs[0].raw_body!, 'hex'))[0].beginParse();
                reportMsgBody.loadCoins() // contract version
                reportMsgBody.loadMaybeRef() // upgrade info
                reportMsgBody.loadInt(2) // upgrade exec
                const reportOp = reportMsgBody.loadUint(32);
                if (reportOp !== 0x11f1 && reportOp !== 0x211a && reportOp !== 0x311a) continue;
                const logMsg = outMsgs.find(msg => msg.msg_type === 'ext_out_msg');
                
                if (logMsg === undefined) {
                    await bot.api.sendMessage(serviceChatID, `🚨🚨🚨 Log message not found for transaction ${hash.toString(16)} 🚨🚨🚨`);
                    throw new Error(`Log message not found for transaction ${hash.toString(16)}`);
                }

                const logBody = Cell.fromBoc(Buffer.from(logMsg.raw_body!, 'hex'))[0].beginParse();

                let log: Log;
                if (op === 0x11a) {
                    const op = logBody.loadUint(8);
                    if (op !== 1) throw new Error(`Invalid op code ${op} for transaction ${hash.toString(16)}`);
                    const userAddress = logBody.loadAddress();
                    const userContractAddress = logBody.loadAddress();
                    const currentTime = logBody.loadUint(32);
                    const attachedAssetData = logBody.loadRef().beginParse();
                    const assetID = attachedAssetData.loadUintBig(256);
                    const amountSupplied = attachedAssetData.loadUintBig(64);
                    const userNewPrincipal = attachedAssetData.loadIntBig(64);
                    const newTotalSupply = attachedAssetData.loadIntBig(64);
                    const newTotalBorrow = attachedAssetData.loadIntBig(64);
                    const sRate = attachedAssetData.loadUintBig(64);
                    const bRate = attachedAssetData.loadUintBig(64);
                    attachedAssetData.endParse();
                    const redeemedAssetData = logBody.loadRef().beginParse();
                    redeemedAssetData.endParse();
                    logBody.endParse();   
                    log = {
                        utime: currentTime,
                        hash,
                        txType: TxType.SUPPLY,
                        senderAddress: userContractAddress,
                        userAddress: userAddress,
                        attachedAssetAddress: assetID,
                        attachedAssetAmount: amountSupplied,
                        attachedAssetPrincipal: userNewPrincipal,
                        attachedAssetTotalSupplyPrincipal: newTotalSupply,
                        attachedAssetTotalBorrowPrincipal: newTotalBorrow,
                        attachedAssetSRate: sRate,
                        attachedAssetBRate: bRate
                    }

                    await retry(async () => await db.addAttachedAssetLog(log), 5, 1000, '[Add supply log]');
                    console.log(`Supply log added for transaction ${hash.toString(16)}`);
                } 
                
                if (op === 0x211) {
                    const op = logBody.loadUint(8);
                    if (op !== 2) throw new Error(`Invalid op code ${op} for transaction ${hash.toString(16)}`);
                    const userAddress = logBody.loadAddress();
                    const userContractAddress = logBody.loadAddress();
                    const currentTime = logBody.loadUint(32);
                    const supplyAssetData = logBody.loadRef().beginParse();
                    supplyAssetData.endParse();
                    const attachedAssetData = logBody.loadRef().beginParse();
                    const assetID = attachedAssetData.loadUintBig(256);
                    const withdrawAmountCurrent = attachedAssetData.loadUintBig(64);
                    const userNewPrincipal = attachedAssetData.loadIntBig(64);
                    const newTotalSupply = attachedAssetData.loadIntBig(64);
                    const newTotalBorrow = attachedAssetData.loadIntBig(64);
                    const sRate = attachedAssetData.loadUintBig(64);
                    const bRate = attachedAssetData.loadUintBig(64);
                    attachedAssetData.endParse();
                    logBody.endParse();

                    log = {
                        utime: currentTime,
                        hash,
                        txType: TxType.WITHDRAW,
                        senderAddress: userContractAddress,
                        userAddress: userAddress,
                        redeemedAssetAddress: assetID,
                        redeemedAssetAmount: withdrawAmountCurrent,
                        redeemedAssetPrincipal: userNewPrincipal,
                        redeemedAssetTotalSupplyPrincipal: newTotalSupply,
                        redeemedAssetTotalBorrowPrincipal: newTotalBorrow,
                        redeemedAssetSRate: sRate,
                        redeemedAssetBRate: bRate
                    }

                    await retry(async () => await db.addRedeemedAssetLog(log), 5, 1000, '[Add withdraw log]');
                    console.log(`Withdraw log added for transaction ${hash.toString(16)}`);
                } 
                
                if (op === 0x311) {
                    const op = logBody.loadUint(8);
                    if (op !== 3) throw new Error(`Invalid op code ${op} for transaction ${hash.toString(16)}`);
                    const userAddress = logBody.loadAddress();
                    const userContractAddress = logBody.loadAddress();
                    const currentTime = logBody.loadUint(32);
                    const attachedAssetData = logBody.loadRef().beginParse();
                    const transferredAssetID = attachedAssetData.loadUintBig(256);
                    const transferredAmount = attachedAssetData.loadUintBig(64);
                    const newUserLoanPrincipal = attachedAssetData.loadIntBig(64);
                    const loanNewTotalSupply = attachedAssetData.loadIntBig(64);
                    const loanNewTotalBorrow = attachedAssetData.loadIntBig(64);
                    const loanSRate = attachedAssetData.loadUintBig(64);
                    const loanBRate = attachedAssetData.loadUintBig(64);
                    attachedAssetData.endParse();
                    const redeemedAssetData = logBody.loadRef().beginParse();
                    const collateralAssetID = redeemedAssetData.loadUintBig(256);
                    const collateralReward = redeemedAssetData.loadUintBig(64);
                    const newUserCollateralPrincipal = redeemedAssetData.loadIntBig(64);
                    const newCollateralTotalSupply = redeemedAssetData.loadIntBig(64);
                    const newCollateralTotalBorrow = redeemedAssetData.loadIntBig(64);
                    const collateralSRate = redeemedAssetData.loadUintBig(64);
                    const collateralBRate = redeemedAssetData.loadUintBig(64);
                    redeemedAssetData.endParse();
                    logBody.endParse();

                    log = {
                        utime: currentTime,
                        hash,
                        txType: TxType.LIQUIDATE,
                        senderAddress: userContractAddress,
                        userAddress: userAddress,
                        attachedAssetAddress: transferredAssetID,
                        attachedAssetAmount: transferredAmount,
                        attachedAssetPrincipal: newUserLoanPrincipal,
                        attachedAssetTotalSupplyPrincipal: loanNewTotalSupply,
                        attachedAssetTotalBorrowPrincipal: loanNewTotalBorrow,
                        attachedAssetSRate: loanSRate,
                        attachedAssetBRate: loanBRate,
                        redeemedAssetAddress: collateralAssetID,
                        redeemedAssetAmount: collateralReward,
                        redeemedAssetPrincipal: newUserCollateralPrincipal,
                        redeemedAssetTotalSupplyPrincipal: newCollateralTotalSupply,
                        redeemedAssetTotalBorrowPrincipal: newCollateralTotalBorrow,
                        redeemedAssetSRate: collateralSRate,
                        redeemedAssetBRate: collateralBRate
                    }

                    await retry(async () => await db.addLog(log), 5, 1000, '[Add liquidate log]');
                    console.log(`Liquidate log added for transaction ${hash.toString(16)}`);
                }
            }
            else {
                continue;
            }
        }

        console.log(`Before lt: ${before_lt}`);
        await sleep(1500);
    }
}
