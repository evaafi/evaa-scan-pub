import { Address, beginCell, DictionaryValue } from '@ton/core';
import { Slice } from '@ton/ton';

export function getFriendlyAddress(address: string | Address): string {
    if (typeof address === 'string') {
        address = Address.parse(address);
    }
    return address.toString({
        bounceable: true,
        urlSafe: true,
        testOnly: false,
    });
}

export function getRequest(address: Address, before_lt: number, limit: number, after_lt: number = 0) {
    if (before_lt === 0 && after_lt === 0)
        return `v2/blockchain/accounts/${address.toRawString()}/transactions?limit=${limit}`;
    else if (before_lt === 0)
        return `v2/blockchain/accounts/${address.toRawString()}/transactions?after_lt=${after_lt}&limit=${limit}`;
    else if (after_lt === 0)
        return `v2/blockchain/accounts/${address.toRawString()}/transactions?before_lt=${before_lt}&limit=${limit}`;
    else
        return `v2/blockchain/accounts/${address.toRawString()}/transactions?before_lt=${before_lt}&after_lt=${after_lt}&limit=${limit}`;
}

export function createAssetData(): DictionaryValue<AssetData> {
    return {
        serialize: (src: any, buidler: any) => {
            buidler.storeUint(src.s_rate, 64);
            buidler.storeUint(src.b_rate, 64);
            buidler.storeUint(src.totalSupply, 64);
            buidler.storeUint(src.totalBorrow, 64);
            buidler.storeUint(src.lastAccural, 32);
            buidler.storeUint(src.balance, 64);
        },
        parse: (src: Slice) => {
            const sRate = BigInt(src.loadInt(64));
            const bRate = BigInt(src.loadInt(64));
            const totalSupply = BigInt(src.loadInt(64));
            const totalBorrow = BigInt(src.loadInt(64));
            const lastAccural = BigInt(src.loadInt(32));
            const balance = BigInt(src.loadInt(64));
            return { sRate, bRate, totalSupply, totalBorrow, lastAccural, balance };
        },
    };
}

export type AssetConfig = {
    oracle: bigint;
    decimals: bigint;
    collateralFactor: bigint;
    liquidationThreshold: bigint;
    liquidationBonus: bigint;
    baseBorrowRate: bigint;
    borrowRateSlopeLow: bigint;
    borrowRateSlopeHigh: bigint;
    supplyRateSlopeLow: bigint;
    supplyRateSlopeHigh: bigint;
    targetUtilization: bigint;
    originationFee: bigint;
    dust: bigint;
};

export type AssetData = {
    sRate: bigint;
    bRate: bigint;
    totalSupply: bigint;
    totalBorrow: bigint;
    lastAccural: bigint;
    balance: bigint;
};

export function createAssetConfig(): DictionaryValue<AssetConfig> {
    return {
        serialize: (src: any, builder: any) => {
            builder.storeUint(src.oracle, 256);
            builder.storeUint(src.decimals, 8);
            const refBuild = beginCell();
            refBuild.storeUint(src.collateralFactor, 16);
            refBuild.storeUint(src.liquidationThreshold, 16);
            refBuild.storeUint(src.liquidationPenalty, 16);
            refBuild.storeUint(src.baseBorrowRate, 64);
            refBuild.storeUint(src.borrowRateSlopeLow, 64);
            refBuild.storeUint(src.borrowRateSlopeHigh, 64);
            refBuild.storeUint(src.supplyRateSlopeLow, 64);
            refBuild.storeUint(src.supplyRateSlopeHigh, 64);
            refBuild.storeUint(src.targetUtilization, 64);
            refBuild.storeUint(src.originationFee, 64);
            builder.storeRef(refBuild.endCell());
        },
        parse: (src: Slice) => {
            const oracle = src.loadUintBig(256);
            const decimals = BigInt(src.loadUint(8));
            const ref = src.loadRef().beginParse();
            const collateralFactor = ref.loadUintBig(16);
            const liquidationThreshold = ref.loadUintBig(16);
            const liquidationBonus = ref.loadUintBig(16);
            const baseBorrowRate = ref.loadUintBig(64);
            const borrowRateSlopeLow = ref.loadUintBig(64);
            const borrowRateSlopeHigh = ref.loadUintBig(64);
            const supplyRateSlopeLow = ref.loadUintBig(64);
            const supplyRateSlopeHigh = ref.loadUintBig(64);
            const targetUtilization = ref.loadUintBig(64);
            const originationFee = ref.loadUintBig(64);
            const dust = ref.loadUintBig(64);

            return {
                oracle,
                decimals,
                collateralFactor,
                liquidationThreshold,
                liquidationBonus,
                baseBorrowRate,
                borrowRateSlopeLow,
                borrowRateSlopeHigh,
                supplyRateSlopeLow,
                supplyRateSlopeHigh,
                targetUtilization,
                originationFee,
                dust,
            };
        },
    };
}
