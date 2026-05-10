import { Routes } from '@angular/router';

import { SetupPage } from "./features/setup/setup-page/setup-page";

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
];
