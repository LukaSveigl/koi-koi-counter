import { Component, computed, inject } from '@angular/core';
import { GameStore } from '../../../core/store/game.store';
import { Router } from '@angular/router';

@Component({
  selector: 'app-summary-page',
  standalone: true,
  templateUrl: './summary-page.html',
  styleUrl: './summary-page.scss',
})
export class SummaryPage {
  private readonly gameStore = inject(GameStore);

  private readonly router = inject(Router);

  readonly players = this.gameStore.players;

  readonly roundHistory = this.gameStore.roundHistory;

  resetGame() {
    this.gameStore.resetGame();
    void this.router.navigate(['/setup']);
  }
}
