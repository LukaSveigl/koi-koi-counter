import { Routes } from '@angular/router';

import { SetupPage } from './features/setup/setup-page/setup-page';
import { RoundPage } from './features/gameplay/round-page/round-page';
import { SummaryPage } from './features/summary/summary-page/summary-page';

export const routes: Routes = [
  {
    path: '',
    redirectTo: 'setup',
    pathMatch: 'full',
  },
  {
    path: 'setup',
    component: SetupPage,
  },
  {
    path: 'game',
    component: RoundPage,
  },
  {
    path: 'summary',
    component: SummaryPage,
  },
];
