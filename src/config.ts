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


export const isTestnet = false;

export const serviceChatID = process.env.CHAT_ID;