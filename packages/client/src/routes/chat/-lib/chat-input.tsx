import type { ChatId } from "@app/domain/api/chat-rpc";
import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { ArrowUpIcon, StopCircleIcon } from "lucide-react";
import * as React from "react";
import { Button } from "react-aria-components";
import { generatingAtom, inputAtom, interruptAtom, sendMessageAtom } from "./chat-atoms.js";

export const ChatInput = ({ chatId }: { readonly chatId: ChatId; }) => {
  const [input, setInput] = useAtom(inputAtom);
  const isGenerating = useAtomValue(generatingAtom);
  const sendMessage = useAtomSet(sendMessageAtom);
  const interrupt = useAtomSet(interruptAtom);
  const textareaRef = React.useRef<HTMLTextAreaElement>(null);

  const handleSubmit = () => {
    const trimmed = input.trim();
    if (!trimmed || isGenerating) return;
    sendMessage({ chatId, message: trimmed });
    setInput("");
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const handleInput = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);
    const textarea = e.target;
    textarea.style.height = "auto";
    textarea.style.height = Math.min(textarea.scrollHeight, 200) + "px";
  };

  return (
    <div className="p-4">
      <div className="max-w-3xl mx-auto rounded-2xl bg-surface border border-border overflow-hidden">
        <textarea
          ref={textareaRef}
          value={input}
          onChange={handleInput}
          onKeyDown={handleKeyDown}
          placeholder="Send a message..."
          rows={1}
          className="w-full bg-transparent px-4 pt-3 pb-1 resize-none outline-none text-foreground placeholder:text-dimmed min-h-[44px] max-h-[200px]"
        />
        <div className="flex items-center justify-end px-3 pb-2">
          {isGenerating
            ? (
              <Button
                onPress={() => {
                  interrupt(chatId);
                }}
                className="p-2 rounded-lg bg-danger/20 text-danger hover:bg-danger/30 transition-colors cursor-pointer"
              >
                <StopCircleIcon className="size-4" />
              </Button>
            )
            : (
              <Button
                onPress={handleSubmit}
                isDisabled={!input.trim()}
                className="p-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary-hover transition-colors disabled:opacity-40 cursor-pointer disabled:cursor-not-allowed"
              >
                <ArrowUpIcon className="size-4" />
              </Button>
            )}
        </div>
      </div>
    </div>
  );
};
