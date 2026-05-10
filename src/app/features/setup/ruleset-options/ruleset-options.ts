import { Component, input, output } from '@angular/core';

@Component({
  selector: 'app-ruleset-options',
  standalone: true,
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
