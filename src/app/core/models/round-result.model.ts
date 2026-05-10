import { Yaku } from "./yaku.model";

export interface RoundResult {
    roundNumber: number;
    winnerId: string;
    yakus: Yaku[];
    koiKoiCount: number;
    stopped: boolean;
    totalPoints: number;
}