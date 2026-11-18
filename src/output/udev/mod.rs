//! This is module for Backflow's udev virtual device driver.
//!
//! It provides a way to transform input events into special udev events,
//! useful for compositing into an actual InputPlumber composite device.
//!
//!
//! While we used to actually inject input events through InputPlumber through D-Bus,
//! this module provides a robust way to handle these udev events directly, with proper transformations
//! through InputPlumber's composite driver.

use evdev::KeyCode;
use eyre::Result;
use std::collections::HashMap;
use std::str::FromStr;
use uinput::Device;
#[allow(unused_imports)] // May be used in future device implementations
use uinput::event::Code;
use uinput::event::controller::Mouse;
use uinput::event::keyboard::Key;

use crate::input::{
    InputEvent, InputEventPacket, InputEventReceiver, InputEventStream, JoystickEvent,
    KeyboardEvent, PointerEvent,
};
use crate::output::OutputBackend;

/// Enum representing different device types
pub enum DeviceType {
    Keyboard,
    Mouse,
    Gamepad,
}

impl DeviceType {
    #[cfg(test)] // Part of public API, may be used for device type comparison
    pub fn as_str(&self) -> &'static str {
        match self {
            DeviceType::Keyboard => "keyboard",
            DeviceType::Mouse => "mouse",
            DeviceType::Gamepad => "gamepad",
        }
    }
}

/// Represents a virtual udev device entry
pub struct UdevDeviceEntry {
    pub device_type: DeviceType,
    pub device: Device,
}

/// USB bus type advertised by every virtual device (`BUS_USB`).
const VIRTUAL_BUS: u16 = 0x03;
/// Vendor ID shared by Backflow's virtual devices (pid.codes).
const VIRTUAL_VENDOR: u16 = 0x1209;
/// Product IDs. They must stay distinct from each other and from every other
/// uinput device on the host: Wine derives the HID device instance path from
/// `VID_xxxx&PID_xxxx`, so two devices that share `0000:0000` collapse onto one
/// registry node and only one of them is ever enumerated.
const KEYBOARD_PRODUCT: u16 = 0xA1C1;
const MOUSE_PRODUCT: u16 = 0xA1C2;
const GAMEPAD_PRODUCT: u16 = 0xA1C3;

/// Backend for managing udev virtual devices
pub struct UdevOutputBackend {
    pub devices: HashMap<String, UdevDeviceEntry>,
}

impl UdevOutputBackend {
    /// Creates a new `UdevOutputBackend` with virtual devices
    pub fn new() -> Result<Self> {
        let mut devices = HashMap::new();

        // Create keyboard device
        let keyboard_device = Self::create_keyboard_device()?;
        devices.insert(
            "keyboard".to_string(),
            UdevDeviceEntry {
                device_type: DeviceType::Keyboard,
                device: keyboard_device,
            },
        );

        // Create mouse device
        let mouse_device = Self::create_mouse_device()?;
        devices.insert(
            "mouse".to_string(),
            UdevDeviceEntry {
                device_type: DeviceType::Mouse,
                device: mouse_device,
            },
        );

        // Create gamepad device (placeholder for now)
        let gamepad_device = Self::create_gamepad_device()?;
        devices.insert(
            "gamepad".to_string(),
            UdevDeviceEntry {
                device_type: DeviceType::Gamepad,
                device: gamepad_device,
            },
        );

        tracing::info!("Created udev output backend with {} devices", devices.len());

        Ok(Self { devices })
    }

    fn create_keyboard_device() -> Result<Device> {
        tracing::info!("Creating virtual keyboard device...");
        let device = uinput::default()?
            .name("Backflow Virtual Keyboard")?
            .event(uinput::event::Keyboard::All)?
            .bus(VIRTUAL_BUS)
            .vendor(VIRTUAL_VENDOR)
            .product(KEYBOARD_PRODUCT)
            .version(1)
            .create()?;

        tracing::info!("Virtual keyboard device created successfully");
        Ok(device)
    }

    fn create_mouse_device() -> Result<Device> {
        tracing::info!("Creating virtual mouse device...");
        let mut device_builder = uinput::default()?
            .name("Backflow Virtual Mouse")?
            .event(uinput::event::Relative::Position(
                uinput::event::relative::Position::X,
            ))?
            .event(uinput::event::Relative::Position(
                uinput::event::relative::Position::Y,
            ))?
            .event(uinput::event::Relative::Wheel(
                uinput::event::relative::Wheel::Vertical,
            ))?
            .event(uinput::event::Relative::Wheel(
                uinput::event::relative::Wheel::Horizontal,
            ))?
            .bus(VIRTUAL_BUS)
            .vendor(VIRTUAL_VENDOR)
            .product(MOUSE_PRODUCT)
            .version(1);

        // Add mouse button events
        for variant in uinput::event::controller::Mouse::iter_variants() {
            device_builder = device_builder.event(variant)?;
        }

        let device = device_builder.create()?;
        tracing::info!("Virtual mouse device created successfully");
        Ok(device)
    }

    fn create_gamepad_device() -> Result<Device> {
        tracing::info!("Creating virtual gamepad device...");
        // Basic gamepad with some common buttons and axes
        let device = uinput::default()?
            .name("Backflow Virtual Gamepad")?
            .event(uinput::event::controller::GamePad::A)?
            .event(uinput::event::controller::GamePad::B)?
            .event(uinput::event::controller::GamePad::X)?
            .event(uinput::event::controller::GamePad::Y)?
            .event(uinput::event::Absolute::Position(
                uinput::event::absolute::Position::X,
            ))?
            .min(-32768)
            .max(32767)
            .event(uinput::event::Absolute::Position(
                uinput::event::absolute::Position::Y,
            ))?
            .min(-32768)
            .max(32767)
            .bus(VIRTUAL_BUS)
            .vendor(VIRTUAL_VENDOR)
            .product(GAMEPAD_PRODUCT)
            .version(1)
            .create()?;

        tracing::info!("Virtual gamepad device created successfully");
        Ok(device)
    }

    /*     /// Get a device by its type string
       pub fn get_device(&self, device_type: &str) -> Option<&UdevDeviceEntry> {
           self.devices.get(device_type)
       }
    */
    /// Get a mutable reference to a device by its type string
    pub fn get_device_mut(&mut self, device_type: &str) -> Option<&mut UdevDeviceEntry> {
        self.devices.get_mut(device_type)
    }

    /*     /// Get the keyboard device if it exists
    pub fn keyboard(&self) -> Option<&UdevDeviceEntry> {
        self.get_device("keyboard")
    }

    /// Get the mouse device if it exists
    pub fn mouse(&self) -> Option<&UdevDeviceEntry> {
        self.get_device("mouse")
    }

    /// Get the gamepad device if it exists
    pub fn gamepad(&self) -> Option<&UdevDeviceEntry> {
        self.get_device("gamepad")
    } */
}

/// Main udev output implementation
pub struct UdevOutput {
    /// Input event receiver (dedicated for this backend)
    pub receiver: InputEventReceiver,
    /// Udev backend for managing virtual devices
    pub backend: UdevOutputBackend,
}

impl UdevOutput {
    /// Creates a new `UdevOutput` with the given input event stream
    pub fn new(stream: InputEventStream) -> Result<Self> {
        Ok(Self {
            receiver: stream.subscribe(),
            backend: UdevOutputBackend::new()?,
        })
    }

    async fn process_packet(&mut self, packet: InputEventPacket) -> Result<()> {
        tracing::trace!("Received input event packet: {:?}", packet);

        // Group events by device type for batch processing
        let mut keyboard_events = Vec::new();
        let mut mouse_events = Vec::new();
        let mut gamepad_events = Vec::new();

        for event in packet.events {
            match event {
                InputEvent::Keyboard(e) => keyboard_events.push(e),
                InputEvent::Pointer(e) => mouse_events.push(e),
                InputEvent::Joystick(e) => gamepad_events.push(e),
                InputEvent::Analog(_) => {} // skip for now
            }
        }

        // Process events in batch
        self.process_keyboard_events(&keyboard_events)?;
        self.process_mouse_events(&mouse_events)?;
        self.process_gamepad_events(&gamepad_events)?;

        Ok(())
    }

    fn process_keyboard_events(&mut self, events: &[KeyboardEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let Some(kb_device) = self.backend.get_device_mut("keyboard") else {
            tracing::warn!("No keyboard device available");
            return Ok(());
        };

        for event in events {
            match event {
                KeyboardEvent::KeyPress { key } => {
                    if let Ok(uinput_key) = Self::evdev_key_to_uinput_key(key) {
                        kb_device.device.press(&uinput_key)?;
                    } else {
                        tracing::warn!("Unknown key: {}", key);
                    }
                }
                KeyboardEvent::KeyRelease { key } => {
                    if let Ok(uinput_key) = Self::evdev_key_to_uinput_key(key) {
                        kb_device.device.release(&uinput_key)?;
                    } else {
                        tracing::warn!("Unknown key: {}", key);
                    }
                }
            }
        }
        kb_device.device.synchronize()?;
        Ok(())
    }

    fn process_mouse_events(&mut self, events: &[PointerEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let Some(mouse_device) = self.backend.get_device_mut("mouse") else {
            tracing::warn!("No mouse device available");
            return Ok(());
        };

        for event in events {
            match event {
                PointerEvent::Move { x_delta, y_delta } => {
                    mouse_device.device.send(
                        uinput::Event::Relative(uinput::event::Relative::Position(
                            uinput::event::relative::Position::X,
                        )),
                        *x_delta,
                    )?;
                    mouse_device.device.send(
                        uinput::Event::Relative(uinput::event::Relative::Position(
                            uinput::event::relative::Position::Y,
                        )),
                        *y_delta,
                    )?;
                }
                PointerEvent::Click { button } => {
                    if let Some(mouse_button) = Self::button_to_mouse_button(*button) {
                        mouse_device.device.send(
                            uinput::Event::Controller(uinput::event::Controller::Mouse(
                                mouse_button,
                            )),
                            1,
                        )?;
                    } else {
                        tracing::warn!("Unsupported mouse button: {}", button);
                    }
                }
                PointerEvent::ClickRelease { button } => {
                    if let Some(mouse_button) = Self::button_to_mouse_button(*button) {
                        mouse_device.device.send(
                            uinput::Event::Controller(uinput::event::Controller::Mouse(
                                mouse_button,
                            )),
                            0,
                        )?;
                    } else {
                        tracing::warn!("Unsupported mouse button: {}", button);
                    }
                }
                PointerEvent::Scroll { x_delta, y_delta } => {
                    if *y_delta != 0 {
                        mouse_device.device.send(
                            uinput::Event::Relative(uinput::event::Relative::Wheel(
                                uinput::event::relative::Wheel::Vertical,
                            )),
                            *y_delta,
                        )?;
                    }
                    if *x_delta != 0 {
                        mouse_device.device.send(
                            uinput::Event::Relative(uinput::event::Relative::Wheel(
                                uinput::event::relative::Wheel::Horizontal,
                            )),
                            *x_delta,
                        )?;
                    }
                }
            }
        }
        mouse_device.device.synchronize()?;
        Ok(())
    }

    fn process_gamepad_events(&mut self, events: &[JoystickEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let Some(gamepad_device) = self.backend.get_device_mut("gamepad") else {
            tracing::warn!("No gamepad device available");
            return Ok(());
        };

        for event in events {
            match event {
                JoystickEvent::ButtonPress { button } => {
                    if let Some(gamepad_button) = Self::button_to_gamepad_button(*button) {
                        gamepad_device.device.press(&gamepad_button)?;
                    } else {
                        tracing::warn!("Unsupported gamepad button: {}", button);
                    }
                }
                JoystickEvent::ButtonRelease { button } => {
                    if let Some(gamepad_button) = Self::button_to_gamepad_button(*button) {
                        gamepad_device.device.release(&gamepad_button)?;
                    } else {
                        tracing::warn!("Unsupported gamepad button: {}", button);
                    }
                }
                JoystickEvent::AxisMovement { stick: _, x, y } => {
                    gamepad_device.device.send(
                        uinput::Event::Absolute(uinput::event::Absolute::Position(
                            uinput::event::absolute::Position::X,
                        )),
                        *x as i32,
                    )?;
                    gamepad_device.device.send(
                        uinput::Event::Absolute(uinput::event::Absolute::Position(
                            uinput::event::absolute::Position::Y,
                        )),
                        *y as i32,
                    )?;
                }
            }
        }
        gamepad_device.device.synchronize()?;
        Ok(())
    }

    /// Convert evdev key name string to uinput Key using evdev constants for mapping
    fn evdev_key_to_uinput_key(key_str: &str) -> Result<Key> {
        // Parse the evdev key string to get the key code
        let evdev_key = KeyCode::from_str(key_str)
            .map_err(|_| eyre::eyre!("Invalid evdev key: {}", key_str))?;

        // Get the numeric value of the evdev keycode
        let keycode_num = evdev_key.code();

        // Convert to uinput Key using the same numeric value
        // Both evdev and uinput use the same Linux input event codes
        let uinput_key = unsafe {
            // SAFETY: Both evdev::Key and uinput::event::keyboard::Key use the same underlying
            // Linux input event codes. We're transmuting from one representation to another
            // of the same numeric values.
            std::mem::transmute::<u8, Key>(keycode_num.try_into().unwrap())
        };

        // very cursed iteration
        // let uinput_key = {
        //     uinput::event::keyboard::Key::iter_variants()
        //         .find(|e_key| e_key.code() == keycode_num as i32)
        //         .ok_or_else(|| eyre::eyre!("Unknown uinput key for evdev key: {}", key_str))?
        // };

        Ok(uinput_key)
    }

    /// Convert button number to uinput Mouse button
    fn button_to_mouse_button(button: u8) -> Option<Mouse> {
        match button {
            1 => Some(Mouse::Left),
            2 => Some(Mouse::Right),
            3 => Some(Mouse::Middle),
            4 => Some(Mouse::Side),
            5 => Some(Mouse::Extra),
            _ => None,
        }
    }

    /// Convert button number to uinput GamePad button
    fn button_to_gamepad_button(button: u8) -> Option<uinput::event::controller::GamePad> {
        match button {
            0 => Some(uinput::event::controller::GamePad::A),
            1 => Some(uinput::event::controller::GamePad::B),
            2 => Some(uinput::event::controller::GamePad::X),
            3 => Some(uinput::event::controller::GamePad::Y),
            _ => None,
        }
    }
}

impl OutputBackend for UdevOutput {
    async fn run(&mut self) -> Result<()> {
        tracing::info!("Starting udev output backend");

        loop {
            // Use async receive method - much cleaner than try_recv with sleep
            match self.receiver.receive().await {
                Some(packet) => {
                    if let Err(e) = self.process_packet(packet).await {
                        tracing::error!("Failed to process packet: {}", e);
                    }
                }
                None => {
                    tracing::info!("Input stream closed, stopping udev backend");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        tracing::info!("Stopping udev output backend");
        // Devices will be automatically cleaned up when dropped
        self.backend.devices.clear();
        tracing::info!("Udev output backend stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evdev_key_mapping() -> Result<()> {
        assert!(UdevOutput::evdev_key_to_uinput_key("A").is_err());
        assert!(UdevOutput::evdev_key_to_uinput_key("KEY_A").is_ok());
        assert!(UdevOutput::evdev_key_to_uinput_key("KEY_ENTER").is_ok());
        assert!(UdevOutput::evdev_key_to_uinput_key("INVALID").is_err());
        Ok(())
    }

    #[test]
    fn test_button_mappings() {
        assert!(UdevOutput::button_to_mouse_button(1).is_some());
        assert!(UdevOutput::button_to_mouse_button(6).is_none());
        assert!(UdevOutput::button_to_gamepad_button(0).is_some());
        assert!(UdevOutput::button_to_gamepad_button(10).is_none());
    }

    #[test]
    fn test_virtual_device_ids_are_distinct() {
        // A zeroed VID/PID makes Wine collapse every virtual device onto the
        // same `HID\VID_0000&PID_0000` node, so the keyboard is never
        // enumerated as its own HID interface.
        assert_ne!(VIRTUAL_VENDOR, 0);
        assert_ne!(KEYBOARD_PRODUCT, 0);
        assert_ne!(MOUSE_PRODUCT, 0);
        assert_ne!(GAMEPAD_PRODUCT, 0);

        let products = [KEYBOARD_PRODUCT, MOUSE_PRODUCT, GAMEPAD_PRODUCT];
        for (i, a) in products.iter().enumerate() {
            for b in &products[i + 1..] {
                assert_ne!(a, b, "virtual devices must not share a product ID");
            }
        }

        assert_eq!(VIRTUAL_BUS, 0x03, "virtual devices must claim USB");
    }
}
