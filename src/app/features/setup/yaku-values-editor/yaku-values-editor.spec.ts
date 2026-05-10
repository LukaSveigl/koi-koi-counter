import { ComponentFixture, TestBed } from '@angular/core/testing';

import { YakuValuesEditor } from './yaku-values-editor';

describe('YakuValuesEditor', () => {
  let component: YakuValuesEditor;
  let fixture: ComponentFixture<YakuValuesEditor>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [YakuValuesEditor],
    }).compileComponents();

    fixture = TestBed.createComponent(YakuValuesEditor);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
