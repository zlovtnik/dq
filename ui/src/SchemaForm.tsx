import { For, Show } from "solid-js";
import type { FieldSchema } from "./types";
import { fieldValue } from "./schema";

export function SchemaForm(props: {
  schema: FieldSchema;
  parameters: Record<string, unknown>;
  onChange: (parameters: Record<string, unknown>) => void;
  onError: (message: string) => void;
  disabled: boolean;
}) {
  function update(name: string, schema: FieldSchema, value: string) {
    try {
      const next = { ...props.parameters };
      if (value === "") delete next[name];
      else next[name] = fieldValue(schema, value);
      props.onChange(next);
      props.onError("");
    } catch {
      props.onError(
        `Invalid value for ${name}. Use valid JSON for objects and arrays.`,
      );
    }
  }
  function updateEnum(name: string, value: unknown, placeholder: boolean) {
    const next = { ...props.parameters };
    if (placeholder) delete next[name];
    else next[name] = value;
    props.onChange(next);
    props.onError("");
  }
  const display = (name: string, field: FieldSchema) => {
    const value = props.parameters[name];
    return value === undefined
      ? ""
      : field.type === "object" || field.type === "array"
        ? JSON.stringify(value, null, 2)
        : String(value);
  };
  return (
    <div class="schema-form">
      <For each={Object.entries(props.schema.properties || {})}>
        {([name, field]) => (
          <label class="field">
            <span>
              {field.title || name}{" "}
              <Show when={props.schema.required?.includes(name)}>
                <span aria-label="required">*</span>
              </Show>
            </span>
            <Show
              when={field.enum || field.type === "boolean"}
              fallback={
                <Show
                  when={field.type === "object" || field.type === "array"}
                  fallback={
                    <input
                      name={name}
                      disabled={props.disabled}
                      required={props.schema.required?.includes(name)}
                      type={
                        field.type === "number" || field.type === "integer"
                          ? "number"
                          : "text"
                      }
                      step={field.type === "integer" ? "1" : "any"}
                      min={field.minimum}
                      max={field.maximum}
                      value={display(name, field)}
                      onInput={(event) =>
                        update(name, field, event.currentTarget.value)
                      }
                    />
                  }
                >
                  <textarea
                    name={name}
                    disabled={props.disabled}
                    rows={5}
                    value={display(name, field)}
                    onChange={(event) =>
                      update(name, field, event.currentTarget.value)
                    }
                  />
                </Show>
              }
            >
              <select
                name={name}
                disabled={props.disabled}
                required={props.schema.required?.includes(name)}
                value={display(name, field)}
                onChange={(event) => {
                  const index = event.currentTarget.selectedIndex;
                  const options = field.enum || [true, false];
                  updateEnum(name, options[index - 1], index === 0);
                }}
              >
                <option value="">Choose a value</option>
                <For each={field.enum || [true, false]}>
                  {(value) => (
                    <option value={String(value)}>{String(value)}</option>
                  )}
                </For>
              </select>
            </Show>
            <Show when={field.description}>
              <small>{field.description}</small>
            </Show>
          </label>
        )}
      </For>
      <Show when={!Object.keys(props.schema.properties || {}).length}>
        <p>
          No declared form fields. The backend independently validates empty
          parameters.
        </p>
      </Show>
    </div>
  );
}
