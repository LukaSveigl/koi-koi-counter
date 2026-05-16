import { Component, input, output } from '@angular/core';

import { MatCardModule } from '@angular/material/card';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatButtonModule } from '@angular/material/button';
import { MatSelectModule } from '@angular/material/select';
import { MatSlideToggle } from '@angular/material/slide-toggle';

@Component({
  selector: 'app-ruleset-options',
  standalone: true,
  imports: [
    MatCardModule,
    MatInputModule,
    MatRadioModule,
    MatButtonModule,
    MatSelectModule,
    MatSlideToggle,
  ],
  templateUrl: './ruleset-options.html',
  styleUrl: './ruleset-options.scss',
})
export class RulesetOptions {
  readonly koiKoiBonus = input.required<boolean>();

  readonly flowerViewingSake = input.required<boolean>();

  readonly moonViewingSake = input.required<boolean>();

  readonly sevenPointCap = input.required<boolean>();

  readonly koiKoiBonusChange = output<boolean>();

  readonly flowerViewingSakeChange = output<boolean>();

  readonly moonViewingSakeChange = output<boolean>();

  readonly sevenPointCapChange = output<boolean>();
}
