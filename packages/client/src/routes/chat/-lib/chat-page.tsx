import type { ChatId } from "@app/domain/api/chat-rpc";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import * as Atom from "effect/unstable/reactivity/Atom";
import { Loader2Icon } from "lucide-react";
import * as React from "react";
import { chatAtom, watchChatAtom } from "./chat-atoms.js";
import { ChatInput } from "./chat-input.js";
import { MessageList } from "./message-list.js";

export const ChatPage = ({ chatId }: { readonly chatId: ChatId; }) => {
  const chatResult = useAtomValue(chatAtom);
  const setChat = useAtomSet(chatAtom);
  const setWatchChat = useAtomSet(watchChatAtom);

  React.useEffect(() => {
    setChat(chatId);
    return () => {
      setWatchChat(Atom.Interrupt);
    };
  }, [chatId, setChat, setWatchChat]);

  React.useEffect(() => {
    if (!AsyncResult.isSuccess(chatResult)) {
      return;
    }

    setWatchChat({ chatId, activeRunId: chatResult.value.activeRunId });

    return () => {
      setWatchChat(Atom.Interrupt);
    };
  }, [chatId, chatResult, setWatchChat]);

  if (
    AsyncResult.isInitial(chatResult) || (chatResult.waiting && !AsyncResult.isSuccess(chatResult))
  ) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2Icon className="size-6 animate-spin text-muted" />
      </div>
    );
  }

  if (AsyncResult.isFailure(chatResult)) {
    return (
      <div className="flex-1 flex items-center justify-center text-danger">
        <p>Failed to load chat</p>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col min-h-0">
      <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
        <h1 className="text-sm font-medium truncate">{chatResult.value.title}</h1>
        <span className="text-xs text-muted bg-elevated px-2 py-0.5 rounded">
          {chatResult.value.model}
        </span>
      </div>
      <MessageList />
      <ChatInput chatId={chatId} />
    </div>
  );
};
