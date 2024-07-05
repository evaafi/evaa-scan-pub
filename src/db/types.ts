import { Address } from '@ton/core';

export enum TxType {
    SUPPLY = 'supply',
    WITHDRAW = 'withdraw',
    LIQUIDATE = 'liquidate',
}

export type Log = {
    id?: number;
    utime: number;
    txType: TxType;
    senderAddress: Address;
    userAddress: Address;
    outLt: bigint;
    outBody: string;
    attachedAssetAddress: bigint;
    attachedAssetAmount: bigint;
    attachedAssetPrincipal: bigint;
    attachedAssetTotalSupplyPrincipal: bigint;
    attachedAssetTotalBorrowPrincipal: bigint;
    attachedAssetSRate: bigint;
    attachedAssetBRate: bigint;
    redeemedAssetAddress: bigint;
    redeemedAssetAmount: bigint;
    redeemedAssetPrincipal: bigint;
    redeemedAssetTotalSupplyPrincipal: bigint;
    redeemedAssetTotalBorrowPrincipal: bigint;
    redeemedAssetSRate: bigint;
    redeemedAssetBRate: bigint;
    processed: boolean;
};

export type Operation = {
    id: number;
    hash: string;
    opcode: number;
    inMsgLt: bigint;
    inMsgBody: string;
    createdAt: Date;
};

export type Task = {
    id: number;
    hash: string;
    senderAddress: Address;
    utime: number;
    lt: bigint;
    nextMsgLt: bigint;
    contractAddress: Address;
    processed: boolean;
};
