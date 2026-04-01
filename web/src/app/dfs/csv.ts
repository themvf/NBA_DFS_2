function detectDelimiter(line: string): "," | "\t" {
  let inQuotes = false;
  let commaCount = 0;
  let tabCount = 0;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (inQuotes) continue;
    if (ch === ",") commaCount++;
    if (ch === "\t") tabCount++;
  }

  return tabCount > commaCount ? "\t" : ",";
}

export function parseCsvLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;
  const delimiter = detectDelimiter(line);

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        current += "\"";
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === delimiter && !inQuotes) {
      fields.push(current);
      current = "";
      continue;
    }
    current += ch;
  }

  fields.push(current);
  return fields;
}

export function stringifyCsvLine(fields: readonly string[]): string {
  return fields.map((field) => {
    if (!/[",\r\n]/.test(field)) return field;
    return `"${field.replace(/"/g, "\"\"")}"`;
  }).join(",");
}
