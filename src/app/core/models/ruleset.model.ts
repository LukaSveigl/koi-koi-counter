export interface Ruleset {
    rounds: 6 | 12;
    koiKoiBonus: boolean;
    sevenPointCap: boolean;
    flowerViewingSake: boolean;
    moonViewingSake: boolean;
    yakuValues: Record<string, number>;
}