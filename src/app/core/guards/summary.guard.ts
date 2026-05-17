import { inject } from '@angular/core';

import { CanActivateFn, Router } from '@angular/router';

import { GameStore } from '../store/game.store';

export const summaryGuard: CanActivateFn = () => {
  const gameStore = inject(GameStore);

  const router = inject(Router);

  const hasGameHistory = gameStore.roundHistory().length > 0;

  if (!hasGameHistory) {
    void router.navigate(['/']);

    return false;
  }

  return true;
};
