import { Injectable, computed, signal } from '@angular/core';

import { Player } from '../models/player.model';
import { Ruleset } from '../models/ruleset.model';
import { RoundResult } from '../models/round-result.model';
import { GamePhase } from '../models/game-phase.enum';

@Injectable({
  providedIn: 'root',
})
export class GameStore {
  readonly players = signal<Player[]>([]);

  readonly phase = signal<GamePhase>(GamePhase.Setup);

  readonly currentRound = signal(1);

  readonly ruleset = signal<Ruleset | null>(null);

  readonly roundHistory = signal<RoundResult[]>([]);

  readonly oyaPlayerId = signal<string | null>(null);

  readonly totalRounds = computed(() => {
    return this.ruleset()?.rounds ?? 0;
  });

  readonly gameFinished = computed(() => {
    return this.currentRound() >= this.totalRounds();
  });

  startGame(players: Player[], ruleset: Ruleset, oyaPlayerId: string) {
    this.players.set(players);

    this.ruleset.set(ruleset);

    this.oyaPlayerId.set(oyaPlayerId);

    this.currentRound.set(1);

    this.phase.set(GamePhase.Round);
  }

  applyRound(result: RoundResult) {
    this.roundHistory.update((history) => [...history, result]);

    this.players.update((players) =>
      players.map((player) => {
        if (player.id !== result.winnerId) {
          return player;
        }

        return {
          ...player,
          score: player.score + result.totalPoints,
        };
      })
    );

    this.currentRound.update((round) => round + 1);

    if (this.gameFinished()) {
      this.phase.set(GamePhase.Finished);
    }
  }

  resetGame() {
    this.players.set([]);

    this.roundHistory.set([]);

    this.phase.set(GamePhase.Setup);

    this.currentRound.set(1);

    this.ruleset.set(null);

    this.oyaPlayerId.set(null);
  }
}
