import type { FieldSchema } from "./types";

export function fieldValue(schema: FieldSchema, value: string): unknown {
  if (schema.type === "integer" || schema.type === "number") {
    if (value.trim() === "" || !Number.isFinite(Number(value)))
      throw new Error("Enter a finite number.");
    return Number(value);
  }
  if (schema.type === "boolean") return value === "true";
  if (schema.type === "object" || schema.type === "array")
    return JSON.parse(value);
  return value;
}

// The form checks common scalar constraints. The manager validates the complete pinned schema.
export function checkParameters(
  schema: FieldSchema,
  parameters: Record<string, unknown>,
): string[] {
  const errors: string[] = [];
  for (const key of schema.required || [])
    if (!(key in parameters) || parameters[key] === "")
      errors.push(`${key} is required.`);
  for (const [key, value] of Object.entries(parameters)) {
    const field = schema.properties?.[key];
    if (!field) {
      if (schema.additionalProperties === false)
        errors.push(`${key} is not declared by this version.`);
      continue;
    }
    const actual = Array.isArray(value)
      ? "array"
      : value === null
        ? "null"
        : typeof value;
    if (
      field.type === "integer"
        ? typeof value !== "number" || !Number.isInteger(value)
        : field.type && actual !== field.type
    )
      errors.push(`${key} must be ${field.type}.`);
    if (
      field.enum &&
      !field.enum.some(
        (option) => JSON.stringify(option) === JSON.stringify(value),
      )
    )
      errors.push(`${key} must be an allowed value.`);
    if (
      typeof value === "number" &&
      ((field.minimum !== undefined && value < field.minimum) ||
        (field.maximum !== undefined && value > field.maximum))
    )
      errors.push(`${key} is outside its permitted range.`);
    if (
      typeof value === "string" &&
      ((field.minLength !== undefined && value.length < field.minLength) ||
        (field.maxLength !== undefined && value.length > field.maxLength))
    )
      errors.push(`${key} has an invalid length.`);
  }
  return errors;
}

export function defaultParameters(
  schema: FieldSchema,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [name, field] of Object.entries(schema.properties || {}))
    if (field.default !== undefined) result[name] = field.default;
  return result;
}
