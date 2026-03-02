import { ChatRpc } from "./chat-rpc.js";
import { UsersRpc } from "./users-rpc.js";

export class AppRpc extends UsersRpc.merge(ChatRpc) {}
