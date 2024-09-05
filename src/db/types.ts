import { Address } from "@ton/core";

export type User = {
    id: number,
    wallet_address: string,
    contract_address: string,
    codeVersion: number,
    createdAt: number,
    updatedAt: number,
    tonPrincipal: bigint,
    jusdtPrincipal: bigint,
    jusdcPrincipal: bigint,
    sttonPrincipal: bigint,
    tstonPrincipal: bigint,
    usdtPrincipal: bigint,
    state: string
}

export type Task = {
    id: number;
    walletAddress: string;
    contractAddress: string;
    createdAt: number;
    updatedAt: number;
    loanAsset: bigint;
    collateralAsset: bigint;
    liquidationAmount: bigint;
    minCollateralAmount: bigint;
    pricesCell: string;
    signature: string;
    queryID: bigint;
    state: string;
}

export enum TxType {
    SUPPLY = 'supply',
    WITHDRAW = 'withdraw',
    LIQUIDATE = 'liquidate',
}

export type Log = {
    id?: number;
    hash: bigint;
    utime: number;
    txType: TxType;
    senderAddress: Address;
    userAddress: Address;
    attachedAssetAddress?: bigint;
    attachedAssetAmount?: bigint;
    attachedAssetPrincipal?: bigint;
    attachedAssetTotalSupplyPrincipal?: bigint;
    attachedAssetTotalBorrowPrincipal?: bigint;
    attachedAssetSRate?: bigint;
    attachedAssetBRate?: bigint;
    redeemedAssetAddress?: bigint;
    redeemedAssetAmount?: bigint;
    redeemedAssetPrincipal?: bigint;
    redeemedAssetTotalSupplyPrincipal?: bigint;
    redeemedAssetTotalBorrowPrincipal?: bigint;
    redeemedAssetSRate?: bigint;
    redeemedAssetBRate?: bigint;
};