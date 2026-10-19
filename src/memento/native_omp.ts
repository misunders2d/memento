import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";
import type { Type } from "typebox";
import { installNative, type NativeAPI } from "./native_shared.ts";

export default function memento(omp: ExtensionAPI): void {
  // OMP injects its TypeBox shim. No runtime dependency on Pi's typebox package.
  const native = omp as unknown as NativeAPI;
  const schema = omp.typebox.Type as unknown as typeof Type;
  installNative(native, schema, "omp");
}
