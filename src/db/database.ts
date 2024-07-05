import { Database, open } from 'sqlite';
import sqlite3 from 'sqlite3';
import { Log, Operation, Task, TxType } from './types';
import { getFriendlyAddress } from '../helpers';
import { Address } from '@ton/core';
import { Pool, PoolConfig } from 'pg';

export class MyDatabase {
    private pool: Pool;

    constructor(pgConfig: PoolConfig) {
        this.pool = new Pool(pgConfig);
    }

    async init() {
        await this.pool.query(`
              CREATE TABLE IF NOT EXISTS transactions(
                  id BIGSERIAL PRIMARY KEY,
                  hash VARCHAR NOT NULL UNIQUE,
                  sender_address VARCHAR,
                  utime INTEGER NOT NULL,
                  lt NUMERIC(20, 0) NOT NULL,
                  next_msg_lt NUMERIC(20, 0) NOT NULL,
                  contract_address VARCHAR,
                  processed BOOLEAN DEFAULT FALSE,
                  processed_at TIMESTAMP DEFAULT 'epoch',
                  created_at TIMESTAMP DEFAULT now()
              )
          `);

        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS operations_results(
                id BIGSERIAL PRIMARY KEY,
                hash VARCHAR NOT NULL UNIQUE,
                opcode INTEGER NOT NULL,
                in_msg_lt NUMERIC(20, 0) NOT NULL,
                in_msg_body VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            )
        `);

        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS logs(
                id BIGSERIAL PRIMARY KEY,
                utime INTEGER NOT NULL,
                tx_type VARCHAR NOT NULL,
                sender_address VARCHAR NOT NULL,
                user_address VARCHAR NOT NULL,
                out_lt NUMERIC(20, 0) NOT NULL,
                out_body VARCHAR NOT NULL,
                attached_asset_address NUMERIC(78, 0),
                attached_asset_amount NUMERIC(20, 0),
                attached_asset_principal NUMERIC(19, 0),
                attached_asset_total_supply_principal NUMERIC(19, 0),
                attached_asset_total_borrow_principal NUMERIC(19, 0),
                attached_asset_s_rate NUMERIC(20, 0),
                attached_asset_b_rate NUMERIC(20, 0),
                redeemed_asset_address NUMERIC(78, 0),
                redeemed_asset_amount NUMERIC(20, 0),
                redeemed_asset_principal NUMERIC(19, 0),
                redeemed_asset_total_supply_principal NUMERIC(19, 0),
                redeemed_asset_total_borrow_principal NUMERIC(19, 0),
                redeemed_asset_s_rate NUMERIC(20, 0),
                redeemed_asset_b_rate NUMERIC(20, 0),
                processed BOOLEAN NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            )
      `);

        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS stored_queries (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                query TEXT NOT NULL,
                description TEXT
            );
        `);
    }

    async getFirstTransactionLt(): Promise<bigint> {
        const result = await this.pool.query(
            `
            SELECT lt FROM transactions ORDER BY lt ASC LIMIT 1
        `,
        );

        return BigInt(result.rows[0].lt);
    }

    async addTransaction(
        hash: string,
        utime: number,
        lt: bigint,
        next_msg_lt: bigint,
        processed: boolean = false,
        senderAddress?: Address,
        contractAddress?: Address,
    ) {
        await this.pool.query(
            `
            INSERT INTO transactions(hash, sender_address, utime, lt, next_msg_lt, contract_address, processed) VALUES($1, $2, $3, $4, $5, $6, $7)
        `,
            [
                hash,
                senderAddress ? senderAddress.toString() : null,
                utime,
                lt.toString(),
                next_msg_lt.toString(),
                contractAddress ? contractAddress.toString() : null,
                processed,
            ],
        );

        console.log(`Transaction ${hash} added`);
    }

    async processTransaction(hash: string) {
        await this.pool.query(
            `
            UPDATE transactions SET processed = TRUE, processed_at = now() WHERE hash = $1
        `,
            [hash],
        );
    }

    async getUnprocessedTransactions(): Promise<Task[]> {
        const result = await this.pool.query(
            `
            SELECT * FROM transactions
            WHERE created_at < now() - interval '3 seconds' AND processed = FALSE
        `,
        );

        if (result.rows.length === 0) {
            return [];
        }

        return result.rows.map((row) => ({
            id: row.id,
            hash: row.hash,
            lt: BigInt(row.lt),
            senderAddress: Address.parse(row.sender_address),
            utime: row.utime,
            nextMsgLt: BigInt(row.next_msg_lt),
            contractAddress: Address.parse(row.contract_address),
            processed: row.processed,
        }));
    }

    async isTxExists(hash: string) {
        const result = await this.pool.query(
            `
            SELECT * FROM transactions WHERE hash = $1
        `,
            [hash],
        );
        return result.rows.length > 0;
    }

    async addOperation(operation: Operation) {
        await this.pool.query(
            `
            INSERT INTO operations_results(
                hash,
                opcode,
                in_msg_lt,
                in_msg_body
            ) VALUES ($1, $2, $3, $4)
        `,
            [operation.hash, operation.opcode, operation.inMsgLt.toString(), operation.inMsgBody],
        );
    }

    async getOperation(lt: bigint, rawBody: string): Promise<Operation> {
        const result = await this.pool.query(
            `
            SELECT
                *
            FROM operations_results
            WHERE in_msg_lt = $1 AND in_msg_body = $2
        `,
            [lt.toString(), rawBody],
        );

        if (result.rows.length === 0) {
            return null;
        }

        return {
            id: result.rows[0].id,
            hash: result.rows[0].hash,
            opcode: result.rows[0].opcode,
            inMsgLt: BigInt(result.rows[0].in_msg_lt),
            inMsgBody: result.rows[0].in_msg_body,
            createdAt: result.rows[0].created_at,
        };
    }

    async getUnprocessedLogs(): Promise<Log[]> {
        const result = await this.pool.query(
            `
            SELECT 
                * 
            FROM logs
            WHERE processed = false
        `,
        );

        if (result.rows.length === 0) {
            return [];
        }

        return result.rows.map((row) => ({
            id: row.id,
            utime: row.utime,
            txType: row.tx_type as TxType,
            senderAddress: Address.parse(row.sender_address),
            userAddress: Address.parse(row.user_address),
            outLt: BigInt(row.out_lt),
            outBody: row.out_body,
            attachedAssetAddress: row.attached_asset_address ? BigInt(row.attached_asset_address) : null,
            attachedAssetAmount: row.attached_asset_amount ? BigInt(row.attached_asset_amount) : null,
            attachedAssetPrincipal: row.attached_asset_principal ? BigInt(row.attached_asset_principal) : null,
            attachedAssetTotalSupplyPrincipal: row.attached_asset_total_supply_principal
                ? BigInt(row.attached_asset_total_supply_principal)
                : null,
            attachedAssetTotalBorrowPrincipal: row.attached_asset_total_borrow_principal
                ? BigInt(row.attached_asset_total_borrow_principal)
                : null,
            attachedAssetSRate: row.attached_asset_s_rate ? BigInt(row.attached_asset_s_rate) : null,
            attachedAssetBRate: row.attached_asset_b_rate ? BigInt(row.attached_asset_b_rate) : null,
            redeemedAssetAddress: row.redeemed_asset_address ? BigInt(row.redeemed_asset_address) : null,
            redeemedAssetAmount: row.redeemed_asset_amount ? BigInt(row.redeemed_asset_amount) : null,
            redeemedAssetPrincipal: row.redeemed_asset_principal ? BigInt(row.redeemed_asset_principal) : null,
            redeemedAssetTotalSupplyPrincipal: row.redeemed_asset_total_supply_principal
                ? BigInt(row.redeemed_asset_total_supply_principal)
                : null,
            redeemedAssetTotalBorrowPrincipal: row.redeemed_asset_total_borrow_principal
                ? BigInt(row.redeemed_asset_total_borrow_principal)
                : null,
            redeemedAssetSRate: row.redeemed_asset_s_rate ? BigInt(row.redeemed_asset_s_rate) : null,
            redeemedAssetBRate: row.redeemed_asset_b_rate ? BigInt(row.redeemed_asset_b_rate) : null,
            processed: row.processed,
        }));
    }

    async processLog(id: number) {
        await this.pool.query(
            `
            UPDATE logs SET processed = true WHERE id = $1
        `,
            [id],
        );
    }

    async deleteLog(id: number) {
        await this.pool.query(
            `
            DELETE FROM logs WHERE id = $1
        `,
            [id],
        );
    }

    async addAttachedAssetLog(log: Log) {
        await this.pool.query(
            `
            INSERT INTO logs(
                utime,
                tx_type,
                sender_address,
                user_address,
                out_lt,
                out_body,
                attached_asset_address,
                attached_asset_amount,
                attached_asset_principal,
                attached_asset_total_supply_principal,
                attached_asset_total_borrow_principal,
                attached_asset_s_rate,
                attached_asset_b_rate,
                processed
            ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        `,
            [
                log.utime,
                log.txType,
                getFriendlyAddress(log.senderAddress),
                getFriendlyAddress(log.userAddress),
                log.outLt.toString(),
                log.outBody,
                log.attachedAssetAddress.toString(),
                log.attachedAssetAmount.toString(),
                log.attachedAssetPrincipal.toString(),
                log.attachedAssetTotalSupplyPrincipal.toString(),
                log.attachedAssetTotalBorrowPrincipal.toString(),
                log.attachedAssetSRate.toString(),
                log.attachedAssetBRate.toString(),
                log.processed,
            ],
        );
    }

    async addRedeemedAssetLog(log: Log) {
        await this.pool.query(
            `
                INSERT INTO logs(utime,
                                 tx_type,
                                 sender_address,
                                 user_address,
                                 out_lt,
                                 out_body,
                                 redeemed_asset_address,
                                 redeemed_asset_amount,
                                 redeemed_asset_principal,
                                 redeemed_asset_total_supply_principal,
                                 redeemed_asset_total_borrow_principal,
                                 redeemed_asset_s_rate,
                                 redeemed_asset_b_rate,
                                    processed)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            `,
            [
                log.utime,
                log.txType,
                getFriendlyAddress(log.senderAddress),
                getFriendlyAddress(log.userAddress),
                log.outLt.toString(),
                log.outBody,
                log.redeemedAssetAddress.toString(),
                log.redeemedAssetAmount.toString(),
                log.redeemedAssetPrincipal.toString(),
                log.redeemedAssetTotalSupplyPrincipal.toString(),
                log.redeemedAssetTotalBorrowPrincipal.toString(),
                log.redeemedAssetSRate.toString(),
                log.redeemedAssetBRate.toString(),
                log.processed,
            ],
        );
    }

    async addLog(log: Log) {
        await this.pool.query(
            `
                INSERT INTO logs(utime,
                                 tx_type,
                                 sender_address,
                                 user_address,
                                 out_lt,
                                 out_body,
                                 attached_asset_address,
                                 attached_asset_amount,
                                 attached_asset_principal,
                                 attached_asset_total_supply_principal,
                                 attached_asset_total_borrow_principal,
                                 attached_asset_s_rate,
                                 attached_asset_b_rate,
                                 redeemed_asset_address,
                                 redeemed_asset_amount,
                                 redeemed_asset_principal,
                                 redeemed_asset_total_supply_principal,
                                 redeemed_asset_total_borrow_principal,
                                 redeemed_asset_s_rate,
                                 redeemed_asset_b_rate,
                                 processed)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            `,
            [
                log.utime,
                log.txType,
                getFriendlyAddress(log.senderAddress),
                getFriendlyAddress(log.userAddress),
                log.outLt.toString(),
                log.outBody,
                log.attachedAssetAddress.toString(),
                log.attachedAssetAmount.toString(),
                log.attachedAssetPrincipal.toString(),
                log.attachedAssetTotalSupplyPrincipal.toString(),
                log.attachedAssetTotalBorrowPrincipal.toString(),
                log.attachedAssetSRate.toString(),
                log.attachedAssetBRate.toString(),
                log.redeemedAssetAddress.toString(),
                log.redeemedAssetAmount.toString(),
                log.redeemedAssetPrincipal.toString(),
                log.redeemedAssetTotalSupplyPrincipal.toString(),
                log.redeemedAssetTotalBorrowPrincipal.toString(),
                log.redeemedAssetSRate.toString(),
                log.redeemedAssetBRate.toString(),
                log.processed,
            ],
        );
    }

    async getLogs(): Promise<Log[]> {
        const result = await this.pool.query(
            `
            SELECT * FROM logs
        `,
        );

        if (result.rows.length === 0) {
            return [];
        }

        return result.rows.map((row) => ({
            utime: row.utime,
            txType: row.tx_type as TxType,
            senderAddress: Address.parse(row.sender_address),
            userAddress: Address.parse(row.user_address),
            outLt: BigInt(row.out_lt),
            outBody: row.out_body,
            attachedAssetAddress: row.attached_asset_address ? BigInt(row.attached_asset_address) : null,
            attachedAssetAmount: row.attached_asset_amount ? BigInt(row.attached_asset_amount) : null,
            attachedAssetPrincipal: row.attached_asset_principal ? BigInt(row.attached_asset_principal) : null,
            attachedAssetTotalSupplyPrincipal: row.attached_asset_total_supply_principal
                ? BigInt(row.attached_asset_total_supply_principal)
                : null,
            attachedAssetTotalBorrowPrincipal: row.attached_asset_total_borrow_principal
                ? BigInt(row.attached_asset_total_borrow_principal)
                : null,
            attachedAssetSRate: row.attached_asset_s_rate ? BigInt(row.attached_asset_s_rate) : null,
            attachedAssetBRate: row.attached_asset_b_rate ? BigInt(row.attached_asset_b_rate) : null,
            redeemedAssetAddress: row.redeemed_asset_address ? BigInt(row.redeemed_asset_address) : null,
            redeemedAssetAmount: row.redeemed_asset_amount ? BigInt(row.redeemed_asset_amount) : null,
            redeemedAssetPrincipal: row.redeemed_asset_principal ? BigInt(row.redeemed_asset_principal) : null,
            redeemedAssetTotalSupplyPrincipal: row.redeemed_asset_total_supply_principal
                ? BigInt(row.redeemed_asset_total_supply_principal)
                : null,
            redeemedAssetTotalBorrowPrincipal: row.redeemed_asset_total_borrow_principal
                ? BigInt(row.redeemed_asset_total_borrow_principal)
                : null,
            redeemedAssetSRate: row.redeemed_asset_s_rate ? BigInt(row.redeemed_asset_s_rate) : null,
            redeemedAssetBRate: row.redeemed_asset_b_rate ? BigInt(row.redeemed_asset_b_rate) : null,
            processed: row.processed,
        }));
    }
}
