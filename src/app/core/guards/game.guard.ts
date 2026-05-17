import { inject } from '@angular/core';

import { CanActivateFn, Router } from '@angular/router';

import { GameStore } from '../store/game.store';

export const gameGuard: CanActivateFn = () => {
  const gameStore = inject(GameStore);

  const router = inject(Router);

  const hasPlayers = gameStore.players().length > 0;

  if (!hasPlayers) {
    void router.navigate(['/']);

    return false;
  }

  return true;
};
