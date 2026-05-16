import { Component, input, output } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatCard, MatCardContent, MatCardHeader, MatCardTitle } from '@angular/material/card';
import { MatDivider } from '@angular/material/list';
import { Yaku } from '../../../core/models/yaku.model';

@Component({
  selector: 'app-yaku-selector',
  standalone: true,
  imports: [MatButton, MatCard, MatCardContent, MatCardHeader, MatCardTitle, MatDivider],
  templateUrl: './yaku-selector.html',
  styleUrl: './yaku-selector.scss',
})
export class YakuSelector {
  readonly yakus = input.required<Yaku[]>();

  readonly selectedYakus = input.required<Yaku[]>();

  readonly yakuSelected = output<string>();

  readonly yakuDeselected = output<string>();
}
