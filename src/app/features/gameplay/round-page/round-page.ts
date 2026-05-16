import { Component, computed, inject, signal } from '@angular/core';
import { Router } from '@angular/router';

import { GameStore } from '../../../core/store/game.store';
import { calculateRoundScore } from '../../../core/scoring/score-engine';

import { YAKUS } from '../../../data/yakus';
import { GamePhase } from '../../../core/models/game-phase.enum';
import { Yaku } from '../../../core/models/yaku.model';

@Component({
  selector: 'app-round-page',
  standalone: true,
  templateUrl: './round-page.html',
  styleUrl: './round-page.scss',
})
export class RoundPage {
  private readonly gameStore = inject(GameStore);

  private readonly router = inject(Router);

  readonly currentRound = this.gameStore.currentRound;

  readonly players = this.gameStore.players;

  readonly ruleset = this.gameStore.ruleset;

  readonly winnerId = signal<string>('');

  readonly selectedYakus = signal<Yaku[]>([]);

  readonly koiKoiCount = signal(0);

  readonly yakus = YAKUS;

  readonly roundValue = computed(() => {
    const ruleset = this.ruleset();

    if (!ruleset) {
      return 0;
    }

    return calculateRoundScore(this.selectedYakus(), this.koiKoiCount(), ruleset);
  });

  readonly validRound = computed(() => {
    if (this.players().length === 0) {
      return false;
    } else {
      return (
        this.koiKoiCount() >= 0 &&
        this.players().filter((player) => player.id === this.winnerId()).length
      );
    }
  });

  constructor() {
    if (this.ruleset()?.flowerViewingSake === false) {
      this.yakus = this.yakus.filter((yaku) => yaku.id !== 'flower-viewing-sake');
    }

    if (this.ruleset()?.moonViewingSake === false) {
      this.yakus = this.yakus.filter((yaku) => yaku.id !== 'moon-viewing-sake');
    }
  }

  applyRound() {
    if (this.gameStore.phase() !== GamePhase.Round) {
      void this.router.navigate(['/summary']);
    }

    const roundResult = {
      roundNumber: this.gameStore.currentRound(),
      winnerId: this.winnerId(),
      yakus: this.selectedYakus(),
      koiKoiCount: this.koiKoiCount(),
      stopped: true,
      totalPoints: this.roundValue(),
    };

    this.gameStore.applyRound(roundResult);
    this.selectedYakus.set([]);
    this.koiKoiCount.set(0);
  }

  resetRound() {
    this.winnerId.set('');
    this.selectedYakus.set([]);
    this.koiKoiCount.set(0);
  }

  selectYaku(yakuId: string) {
    this.selectedYakus.update((current) => {
      return [...current, this.yakus.find((yaku) => yaku.id === yakuId)!].sort((a, b) =>
        a.id.localeCompare(b.id)
      );
    });
  }

  deselectYaku(yakuId: string) {
    // Remove the first occurrence of the yaku with the given ID from the selected yakus.
    this.selectedYakus.update((current) => {
      const index = current.findIndex((yaku) => yaku.id === yakuId);

      if (index === -1) {
        return current;
      }

      return [...current.slice(0, index), ...current.slice(index + 1)];
    });
  }

  protected readonly Math = Math;
}
