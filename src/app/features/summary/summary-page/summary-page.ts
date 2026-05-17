import { Component, computed, inject } from '@angular/core';

import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatDividerModule } from '@angular/material/divider';
import { MatTableModule } from '@angular/material/table';

import { GameStore } from '../../../core/store/game.store';
import { Router } from '@angular/router';

@Component({
  selector: 'app-summary-page',
  standalone: true,
  imports: [MatCardModule, MatButtonModule, MatDividerModule, MatTableModule],
  templateUrl: './summary-page.html',
  styleUrl: './summary-page.scss',
})
export class SummaryPage {
  private readonly gameStore = inject(GameStore);

  private readonly router = inject(Router);

  readonly players = this.gameStore.players;

  readonly roundHistory = this.gameStore.roundHistory;

  readonly winner = computed(() => {
    return [...this.players()].sort((a, b) => b.score - a.score)[0];
  });

  resetGame() {
    this.gameStore.resetGame();
    void this.router.navigate(['/setup']);
  }

  getWinnerName(playerId: string) {
    return this.players().find((player) => player.id === playerId)?.name ?? 'Unknown';
  }
}
