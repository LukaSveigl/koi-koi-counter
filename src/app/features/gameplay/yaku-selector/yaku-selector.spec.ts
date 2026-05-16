import { ComponentFixture, TestBed } from '@angular/core/testing';

import { YakuSelector } from './yaku-selector';

describe('YakuSelector', () => {
  let component: YakuSelector;
  let fixture: ComponentFixture<YakuSelector>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [YakuSelector],
    }).compileComponents();

    fixture = TestBed.createComponent(YakuSelector);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
