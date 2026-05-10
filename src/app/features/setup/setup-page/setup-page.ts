import { Component, inject, computed, signal } from '@angular/core';
import { Router } from '@angular/router';

import { GameStore } from '../../../core/store/game.store';

import { SCORE_PRESETS } from '../../../data/score-presets';
import { YAKUS } from '../../../data/yakus';

@Component({
  selector: 'app-setup-page',
  imports: [],
  templateUrl: './setup-page.html',
  styleUrl: './setup-page.scss',
})
export class SetupPage {
  private readonly gameStore = inject(GameStore);

  private readonly router = inject(Router);

  readonly playerNames = signal(['', '']);

  readonly rounds = signal<6 | 12>(12);

  readonly koiKoiBonus = signal(true);

  readonly flowerViewingSake = signal(true);

  readonly moonViewingSake = signal(true);

  readonly sevenPointCap = signal(false);

  readonly oyaIndex = signal(0);

  readonly selectedPresetId = signal('standard');

  readonly customYakuValues = signal<Record<string, number>>({});

  readonly presets = SCORE_PRESETS;

  readonly yakus = YAKUS;

  readonly validSetup = computed(() => {
    return this.playerNames().every((name) => name.trim().length > 0);
  });

  constructor() {
    this.selectPreset('standard');
  }

  startGame() {
    const players = this.playerNames().map((name) => ({
      id: crypto.randomUUID(),
      name,
      score: 0,
    }));

    const ruleset = {
      rounds: this.rounds(),
      koiKoiBonus: this.koiKoiBonus(),
      flowerViewingSake: this.flowerViewingSake(),
      moonViewingSake: this.moonViewingSake(),
      sevenPointCap: this.sevenPointCap(),
      yakuValues: this.customYakuValues(),
    };

    this.gameStore.startGame(players, ruleset, players[this.oyaIndex()].id);

    void this.router.navigate(['/game']);
  }

  updatePlayerName(playerName: string, index: number) {
    this.playerNames.update((names) => {
      const newNames = [...names];
      newNames[index] = playerName;
      return newNames;
    });
  }

  selectPreset(id: string) {
    this.selectedPresetId.set(id);

    const preset = this.presets.find((p) => p.id === id);

    if (!preset) {
      return;
    }

    this.customYakuValues.set({
      ...preset.values,
    });
  }

  updateYakuValue(yakuId: string, value: number) {
    this.customYakuValues.update((values) => ({
      ...values,
      [yakuId]: value,
    }));
  }
}
