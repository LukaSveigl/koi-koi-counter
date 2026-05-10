import { Yaku } from "../models/yaku.model";
import { Ruleset } from "../models/ruleset.model";

export function calculateRoundScore(
    yakus: Yaku[],
    koiKoiCount: number,
    ruleset: Ruleset
): number {

    let total = yakus.reduce((sum, yaku) => {
        const value = ruleset.yakuValues[yaku.id] ?? 0;

        return sum + value;
    }, 0);

    if (ruleset.koiKoiBonus) {
        // First Koi-Koi counts as x2 multiplier, second as x3, etc.
        total *= (koiKoiCount + 1);
    }

    if (ruleset.sevenPointCap && total >= 7) {
        total = 7;
    }

    return total;
}