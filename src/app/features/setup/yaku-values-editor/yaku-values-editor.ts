import { Component, input, output } from '@angular/core';

import { Yaku } from '../../../core/models/yaku.model';

@Component({
  selector: 'app-yaku-values-editor',
  standalone: true,
  templateUrl: './yaku-values-editor.html',
  styleUrl: './yaku-values-editor.scss',
})
export class YakuValuesEditor {
  readonly yakus = input.required<Yaku[]>();

  readonly values = input.required<Record<string, number>>();

  readonly valuesChanged = output<Record<string, number>>();

  updateValue(yakuId: string, value: number) {
    this.valuesChanged.emit({
      ...this.values(),
      [yakuId]: value,
    });
  }
}
