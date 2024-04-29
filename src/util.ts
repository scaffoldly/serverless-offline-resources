export const capitalizeKeys = (obj: {
  [key: string]: any;
}): { [key: string]: any } => {
  if (!obj || typeof obj !== "object") return obj;

  return Object.entries(obj).reduce((acc, [key, value]) => {
    acc[capitalize(key)] = value;
    return acc;
  }, {} as { [key: string]: any });
};

const capitalize = (str: string): string => {
  // capitalize the first letter of str
  return str.charAt(0).toUpperCase() + str.slice(1);
};
