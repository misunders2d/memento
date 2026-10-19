import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { installNative, type NativeAPI } from "./native_shared.ts";

export default function memento(pi: ExtensionAPI): void {
  // The helper uses only the documented common Pi/OMP notification/tool subset;
  // their distinct overload maps cannot express that shared structural surface.
  const native = pi as unknown as NativeAPI;
  installNative(native, Type, "pi");
}
