import { Component, input, output } from '@angular/core';

import { MatCardModule } from '@angular/material/card';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatButtonModule } from '@angular/material/button';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';

import { Yaku } from '../../../core/models/yaku.model';

@Component({
  selector: 'app-yaku-values-editor',
  standalone: true,
  imports: [
    MatCardModule,
    MatInputModule,
    MatRadioModule,
    MatButtonModule,
    MatSelectModule,
    MatFormFieldModule,
  ],
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

  protected readonly Math = Math;
}
