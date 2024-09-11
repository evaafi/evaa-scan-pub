import crypto from "crypto";
import { configDotenv } from "dotenv";

configDotenv();

export function sha256Hash(input: string): bigint {
    const hash = crypto.createHash('sha256');
    hash.update(input);
    const hashBuffer = hash.digest();
    const hashHex = hashBuffer.toString('hex');
    return BigInt('0x' + hashHex);
}


export const rpcEndpoint = 'https://rpc.evaa.finance/api/v2/jsonRPC'
export const tonApiEndpoint = 'https://tonapi.io/';
export const isTestnet = false;

export const serviceChatID = process.env.CHAT_ID;