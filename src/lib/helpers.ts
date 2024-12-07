export const resolutionToSeconds = (resolution: string) => {
  const units = resolution.match(/[a-zA-Z]+/);
  const amount = resolution.match(/\d+/);

  let multiplier: number;

  if (units) {
    switch (units[0]) {
      case "S": // Seconds
        multiplier = 1;
        break;
      case "": // No unit, default to minutes
      case "M": // Minutes
        multiplier = 60;
        break;
      case "H": // Hours
        multiplier = 3600;
        break;
      case "D": // Days
        multiplier = 86400;
        break;
      case "W": // Weeks
        multiplier = 604800;
        break;
      case "M": // Months
      case "MO":
        multiplier = 2592000; // Approximate month as 30 days
        break;
      default:
        throw new Error(`Unsupported resolution unit: ${units[0]}`);
    }
  } else {
    // No unit, default to minutes
    multiplier = 60;
  }

  const intAmount = amount ? parseInt(amount[0]) : 1;

  return intAmount * multiplier;
};




