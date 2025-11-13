import { createWalletClient, http } from "viem";
import { TESTNET } from "./testnet.ts";
import { config } from "./config.ts";
import { privateKeyToAccount } from "viem/accounts";
import { getSmartAccountWalletClient, getWalletClient } from "@graphprotocol/grc-20";

const account = privateKeyToAccount(config.pk as `0x${string}`);

export const wallet = createWalletClient({
	account: account,
	chain: TESTNET,
	transport: http(config.rpc, { batch: true }),
});