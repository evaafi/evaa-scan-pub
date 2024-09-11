import { Address } from "@ton/core";
import { UserPrincipals } from "../services/indexer/types";

export type User = {
    id: number,
    wallet_address: string,
    contract_address: string,
    codeVersion: number,
    createdAt: number,
    updatedAt: number,
    principals: UserPrincipals,
    state: bigint
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