import {TupleReader} from "@ton/core";
import {PoolAssetConfig} from "@evaafi/sdk"

export type GetResult = {
    gas_used: number;
    stack: TupleReader;
    exit_code: number;
};

export type UserPrincipals = Map<PoolAssetConfig, bigint>;